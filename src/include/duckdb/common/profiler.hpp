//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/profiler.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/chrono.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/main/perf_counters.hpp"

namespace duckdb {

//! The profiler can be used to measure elapsed time
class Profiler {
public:
	//! Starts the timer
	void Start() {
		finished = false;
		start = Tick();
		perf_counters.StartCounting();
	}
	//! Finishes timing
	void End() {
		end = Tick();
		finished = true;
		perf_counters.StopCounting();
	}

	//! Returns the elapsed time in seconds. If End() has been called, returns
	//! the total elapsed time. Otherwise returns how far along the timer is
	//! right now.
	double Elapsed() const {
		auto _end = finished ? end : Tick();
		return std::chrono::duration_cast<std::chrono::duration<double>>(_end - start).count();
	}

	long long GetNumInstructions() {
		return perf_counters.GetNumInstructions();
	}

	long long GetNumCycles() {
		return perf_counters.GetNumCycles();
	}

private:
	time_point<system_clock> Tick() const {
		return system_clock::now();
	}
	time_point<system_clock> start;
	time_point<system_clock> end;
	bool finished = false;
	PerfCounters perf_counters;
};
} // namespace duckdb
