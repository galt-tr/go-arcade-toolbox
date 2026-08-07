package perf

import (
	"bufio"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
)

// captureEnvironment collects machine + toolchain facts for the run record. All
// probes are best-effort: a missing source yields an empty field, never an
// error.
func captureEnvironment() Environment {
	env := Environment{
		OS:            runtime.GOOS,
		Arch:          runtime.GOARCH,
		LogicalCores:  runtime.NumCPU(),
		GoVersion:     runtime.Version(),
		CPUModel:      cpuModel(),
		RAMBytes:      ramBytes(),
		Kernel:        kernelRelease(),
		PodmanVersion: podmanVersion(),
	}
	if h, err := os.Hostname(); err == nil {
		env.Hostname = h
	}
	return env
}

// cpuModel reads the first "model name" from /proc/cpuinfo (Linux). Empty on
// other platforms.
func cpuModel() string {
	f, err := os.Open("/proc/cpuinfo")
	if err != nil {
		return ""
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "model name") {
			if _, v, ok := strings.Cut(line, ":"); ok {
				return strings.TrimSpace(v)
			}
		}
	}
	return ""
}

// ramBytes reads MemTotal (kB) from /proc/meminfo (Linux). 0 elsewhere.
func ramBytes() uint64 {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "MemTotal:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				if kb, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
					return kb * 1024
				}
			}
		}
	}
	return 0
}

// kernelRelease returns the running kernel version (Linux). Empty elsewhere.
func kernelRelease() string {
	if b, err := os.ReadFile("/proc/sys/kernel/osrelease"); err == nil {
		return strings.TrimSpace(string(b))
	}
	return ""
}

// podmanVersion runs `podman --version` best-effort.
func podmanVersion() string {
	out, err := exec.Command("podman", "--version").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}
