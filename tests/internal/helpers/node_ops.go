package helpers

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1 "k8s.io/api/core/v1"
)

// RunOnNode executes a command on the specified node using
// "oc debug node/<name> -- chroot /host <cmd>".
func RunOnNode(
	ctx context.Context, nodeName string, timeout time.Duration,
	cmd ...string,
) (string, error) {
	if nodeName == "" {
		return "", fmt.Errorf("RunOnNode: nodeName must not be empty")
	}

	if len(cmd) == 0 {
		return "", fmt.Errorf("RunOnNode: cmd must not be empty")
	}

	args := append(
		[]string{"debug", "node/" + nodeName, "-n", "default", "--", "chroot", "/host"},
		cmd...,
	)

	childCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	command := exec.CommandContext(childCtx, "oc", args...)

	var stdout, stderr bytes.Buffer

	command.Stdout = &stdout
	command.Stderr = &stderr

	if err := command.Run(); err != nil {
		if ctx.Err() != nil {
			return "", fmt.Errorf(
				"oc debug on node %s: parent context expired (stderr: %s)",
				nodeName, stderr.String(),
			)
		}

		if childCtx.Err() == context.DeadlineExceeded {
			return "", fmt.Errorf(
				"oc debug on node %s timed out after %s (stderr: %s)",
				nodeName, timeout, stderr.String(),
			)
		}

		return "", fmt.Errorf(
			"oc debug on node %s failed: %w (stderr: %s)",
			nodeName, err, stderr.String(),
		)
	}

	return strings.TrimSpace(stdout.String()), nil
}

// StopKubelet stops the kubelet service on the target node.
func StopKubelet(
	ctx context.Context, nodeName string, timeout time.Duration,
	logf func(string, ...interface{}),
) error {
	_, err := RunOnNode(ctx, nodeName, timeout,
		"sh", "-c",
		`g=/var/tmp/.medik8s-kubelet-stop-guard; [ -f "$g" ] && exit 0; touch "$g" && systemctl stop kubelet`)
	if err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "connection reset") ||
			strings.Contains(errMsg, "lost connection") ||
			strings.Contains(errMsg, "closed network connection") ||
			strings.Contains(errMsg, "broken pipe") ||
			strings.Contains(errMsg, "transport is closing") {
			logf("StopKubelet(%s): suppressed expected connection-loss error "+
				"(kubelet likely stopped): %v\n", nodeName, err)

			return nil
		}

		return err
	}

	return nil
}

// RemoveKubeletStopGuard removes the guard file left by StopKubelet so
// that a subsequent StopKubelet call on the same node will take effect.
func RemoveKubeletStopGuard(
	ctx context.Context, nodeName string, timeout time.Duration,
) error {
	_, err := RunOnNode(ctx, nodeName, timeout,
		"rm", "-f", "/var/tmp/.medik8s-kubelet-stop-guard")

	return err
}

// StartKubelet attempts to start the kubelet service on the target node.
// NOTE: This uses "oc debug node/" which requires a running kubelet to
// schedule the debug pod. It CANNOT recover a node whose kubelet was
// stopped via StopKubelet. Use it only as a best-effort safety net for
// scenarios where kubelet has already restarted (e.g., after a reboot).
func StartKubelet(
	ctx context.Context, nodeName string, timeout time.Duration,
) error {
	_, err := RunOnNode(ctx, nodeName, timeout, "systemctl", "start", "kubelet")

	return err
}

// GetNodeBootID retrieves the boot_id from /proc on the target node via
// oc debug. Requires a running kubelet; use GetNodeBootIDFromAPI when the
// node is down or recovering.
func GetNodeBootID(
	ctx context.Context, nodeName string, timeout time.Duration,
) (string, error) {
	output, err := RunOnNode(
		ctx, nodeName, timeout, "cat", "/proc/sys/kernel/random/boot_id",
	)
	if err != nil {
		return "", fmt.Errorf(
			"failed to get boot_id from node %s: %w", nodeName, err,
		)
	}

	if output == "" {
		return "", fmt.Errorf("node %s returned empty boot_id", nodeName)
	}

	return output, nil
}

// GetNodeBootIDFromAPI retrieves the boot ID from the node's
// status.nodeInfo.bootID via the Kubernetes API. This works even when
// the node is recovering (kubelet updates the API before oc debug
// becomes available).
func GetNodeBootIDFromAPI(
	ctx context.Context, k8sClient client.Client, nodeName string,
) (string, error) {
	node := &corev1.Node{}
	if err := k8sClient.Get(ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
		return "", err
	}

	bootID := node.Status.NodeInfo.BootID
	if bootID == "" {
		return "", fmt.Errorf("node %s has empty status.nodeInfo.bootID", nodeName)
	}

	return bootID, nil
}

// WaitForNodeNotReady polls until the node's Ready condition is not True.
func WaitForNodeNotReady(
	ctx context.Context, k8sClient client.Client, nodeName string,
	pollInterval, timeout time.Duration,
	logf func(string, ...interface{}),
) error {
	return waitForNodeCondition(
		ctx, k8sClient, nodeName, pollInterval, timeout,
		func(node *corev1.Node) bool { return !IsNodeReady(node) },
		"NotReady", logf,
	)
}

// WaitForNodeReady polls until the node's Ready condition is True.
func WaitForNodeReady(
	ctx context.Context, k8sClient client.Client, nodeName string,
	pollInterval, timeout time.Duration,
	logf func(string, ...interface{}),
) error {
	return waitForNodeCondition(
		ctx, k8sClient, nodeName, pollInterval, timeout,
		IsNodeReady,
		"Ready", logf,
	)
}

func waitForNodeCondition(
	ctx context.Context, k8sClient client.Client, nodeName string,
	pollInterval, timeout time.Duration,
	conditionFn func(*corev1.Node) bool, conditionDesc string,
	logf func(string, ...interface{}),
) error {
	err := wait.PollUntilContextTimeout(
		ctx, pollInterval, timeout, true,
		func(ctx context.Context) (bool, error) {
			node := &corev1.Node{}
			if err := k8sClient.Get(
				ctx, client.ObjectKey{Name: nodeName}, node,
			); err != nil {
				if k8serrors.IsNotFound(err) {
					return false, fmt.Errorf(
						"node %s was deleted during readiness wait: %w",
						nodeName, err)
				}

				if k8serrors.IsForbidden(err) || k8serrors.IsUnauthorized(err) {
					return false, fmt.Errorf(
						"permanent API error fetching node %s: %w",
						nodeName, err)
				}

				logf("waitForNodeCondition(%s, %s): transient API error, retrying: %v\n",
					nodeName, conditionDesc, err)

				return false, nil
			}

			return conditionFn(node), nil
		},
	)
	if err != nil {
		if wait.Interrupted(err) {
			return fmt.Errorf(
				"timed out after %s waiting for node %s to become %s: %w",
				timeout, nodeName, conditionDesc, err)
		}

		return fmt.Errorf(
			"failed waiting for node %s to become %s: %w",
			nodeName, conditionDesc, err)
	}

	return nil
}

// WaitForNodeReboot polls until the node's boot ID (via API) differs
// from the previous boot ID, indicating a reboot occurred.
func WaitForNodeReboot(
	ctx context.Context, k8sClient client.Client, nodeName string,
	previousBootID string, pollInterval, timeout time.Duration,
	logf func(string, ...interface{}),
) error {
	if previousBootID == "" {
		return fmt.Errorf("WaitForNodeReboot(%s): previousBootID must not be empty", nodeName)
	}

	err := wait.PollUntilContextTimeout(
		ctx, pollInterval, timeout, true,
		func(ctx context.Context) (bool, error) {
			currentID, err := GetNodeBootIDFromAPI(
				ctx, k8sClient, nodeName,
			)
			if err != nil {
				if k8serrors.IsNotFound(err) {
					return false, fmt.Errorf("node %s was deleted during reboot wait: %w", nodeName, err)
				}

				if k8serrors.IsForbidden(err) || k8serrors.IsUnauthorized(err) {
					return false, fmt.Errorf(
						"permanent API error fetching node %s: %w",
						nodeName, err)
				}

				logf("WaitForNodeReboot(%s): transient API error, retrying: %v\n",
					nodeName, err)

				return false, nil
			}

			return currentID != "" && currentID != previousBootID, nil
		},
	)
	if err != nil {
		if wait.Interrupted(err) {
			return fmt.Errorf(
				"timed out after %s waiting for node %s to reboot (previous boot ID: %s): %w",
				timeout, nodeName, previousBootID, err)
		}

		return fmt.Errorf(
			"failed waiting for node %s to reboot: %w",
			nodeName, err)
	}

	return nil
}
