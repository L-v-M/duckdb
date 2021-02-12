//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/perf_counters.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#if defined(__linux__)

#include <asm/unistd.h>
#include <cstdint>
#include <linux/perf_event.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <unistd.h>

namespace duckdb {

class PerfCounters {
public:
	PerfCounters() {
		instructions_fd = OpenCounter(PERF_COUNT_HW_INSTRUCTIONS);
		cycles_fd = OpenCounter(PERF_COUNT_HW_CPU_CYCLES);
	}
	PerfCounters(const PerfCounters &) = delete;
	PerfCounters &operator=(const PerfCounters &) = delete;
	PerfCounters(PerfCounters &&) = delete;
	PerfCounters &operator=(PerfCounters &&) = delete;

	void StartCounting() {
		ioctl(group_fd, PERF_EVENT_IOC_RESET, PERF_IOC_FLAG_GROUP);
		ioctl(group_fd, PERF_EVENT_IOC_ENABLE, PERF_IOC_FLAG_GROUP);
	}

	void StopCounting() {
		ioctl(group_fd, PERF_EVENT_IOC_DISABLE, PERF_IOC_FLAG_GROUP);
		if (read(instructions_fd, &instructions_counter, sizeof(long long)) != sizeof(long long)) {
			fprintf(stderr, "Error reading instructions counter\n");
			return;
		}
		if (read(cycles_fd, &cycles_counter, sizeof(long long)) != sizeof(long long)) {
			fprintf(stderr, "Error reading cycles counter\n");
			return;
		}
	}

	long long GetNumInstructions() const {
		return instructions_counter;
	}

	long long GetNumCycles() const {
		return cycles_counter;
	}

	~PerfCounters() {
		Destroy(instructions_fd);
		Destroy(cycles_fd);
	}

private:
	void Destroy(int fd) {
		ioctl(fd, PERF_EVENT_IOC_DISABLE, 0);
		close(fd);
	}

	int OpenCounter(std::uint64_t event) {
		struct perf_event_attr pe;
		int fd;
		memset(&pe, 0, sizeof(struct perf_event_attr));
		pe.type = PERF_TYPE_HARDWARE;
		pe.size = sizeof(struct perf_event_attr);
		pe.config = event;
		pe.disabled = 1;
		pe.exclude_kernel = 1;
		pe.exclude_hv = 1;

		if (group_fd == -1) {
			fd = PerfEventOpenCounter(&pe, 0, -1, -1, 0);
			group_fd = fd;
		} else {
			fd = PerfEventOpenCounter(&pe, 0, -1, group_fd, 0);
		}
		if (fd == -1) {
			fprintf(stderr, "Error opening performance counter %llx\n", pe.config);
			exit(EXIT_FAILURE);
		}
		return fd;
	}

	long PerfEventOpenCounter(struct perf_event_attr *hw_event, pid_t pid, int cpu, int group_fd, unsigned long flags) {
		return syscall(__NR_perf_event_open, hw_event, pid, cpu, group_fd, flags);
	}

	int group_fd {-1};
	int instructions_fd;
	int cycles_fd;
	long long instructions_counter {0};
	long long cycles_counter {0};
};

} // namespace duckdb

#else

namespace duckdb {

class PerfCounters {
public:
	PerfCounters() = default;
	PerfCounters(const PerfCounters &) = delete;
	PerfCounters &operator=(const PerfCounters &) = delete;
	PerfCounters(PerfCounters &&) = delete;
	PerfCounters &operator=(PerfCounters &&) = delete;

	void StartCounting() {
	}

	void StopCounting() {
	}

	long long GetNumInstructions() {
		return 0;
	}
	long long GetNumCycles() {
		return 0;
	}
};

} // namespace duckdb
#endif