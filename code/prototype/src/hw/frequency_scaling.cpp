#include "allscale/runtime/hw/frequency_scaling.h"

#include <algorithm>
#include <map>
#include <iostream>
#include <fstream>

#include "allscale/utils/assert.h"
#include "allscale/utils/finalize.h"
#include "allscale/utils/string_utils.h"

#include "allscale/runtime/hw/core.h"

namespace allscale {
namespace runtime {
#ifndef USE_LINUX_CPUFREQ
namespace hw {

	// a dummy implementation

	namespace dummy {

		const std::vector<Frequency> options = { 1.0_GHz, 1.8_GHz, 2.2_GHz, 2.4_GHz };

		std::vector<Frequency> initialSetup() {
			return std::vector<Frequency>(getNumberAvailableCores(),2.2_GHz);
		}

		std::vector<Frequency> currentFrequencies = initialSetup();

	}

	std::vector<Frequency> getFrequencyOptions(Core) {
		// dummy options
		return dummy::options;
	}

	Frequency getFrequency(Core c) {
		assert_lt(c,dummy::currentFrequencies.size());
		return dummy::currentFrequencies[c];
	}

	bool setFrequency(Core c, Frequency f) {
		assert_lt(c,dummy::currentFrequencies.size());
		assert_true(std::binary_search(dummy::options.begin(),dummy::options.end(),f));
		dummy::currentFrequencies[c] = f;
		return true; // update successful
	}

	void resetFrequency(Core) {
		// nothing to do in the dummy implementation
	}

} // end of namespace hw
#else // USE_LINUX_CPUFREQ
namespace hw {

	constexpr int FREQ_PATH_MAX_LENGTH = 128;
	constexpr const char* FREQ_PATH_STRING = "/sys/devices/system/cpu/cpu%u/cpufreq/%s";
	constexpr const char* FREQ_MIN_STRING = "scaling_min_freq";
	constexpr const char* FREQ_MAX_STRING = "scaling_max_freq";
	constexpr const char* FREQ_CUR_STRING = "scaling_cur_freq";

	#define FREQ_DBG(...) fprintf(stderr, __VA_ARGS__)

	std::vector<Frequency> getFrequencyOptions(Core coreid) {
		static thread_local std::map<Core, std::vector<Frequency>> frequencies_cache;
		if(frequencies_cache.find(coreid) != frequencies_cache.end()) return frequencies_cache[coreid];

		// we need this construct to prevent move construction of the return value so as not to invalidate the vector for cache_inserter
		std::vector<Frequency> frequency_storage;
		auto& frequencies = frequency_storage;

		auto cache_inserter = utils::run_finally([&] {
			frequencies_cache.insert({ coreid, frequencies });
		});

		char path_to_cpufreq[FREQ_PATH_MAX_LENGTH];
		sprintf(path_to_cpufreq, FREQ_PATH_STRING, coreid, "scaling_available_frequencies");
		std::ifstream file(path_to_cpufreq, std::ios::binary);
		testing::getFrequencyOptions_num_file_accesses++;

		if(!file) {
			FREQ_DBG("hw::getFrequencyOptions: Unable to open frequency file for reading for core %u, file %s, reason: %s\n", coreid, path_to_cpufreq, strerror(errno));
			return {};
		}

		while(file) {
			// all frequencies are provided in kHz
			int freq_in_khz = 0;
			file >> freq_in_khz;
			if(file) frequencies.push_back(Frequency::kHz(freq_in_khz));
		}

		if(frequencies.empty()) {
			FREQ_DBG("hw::getFrequencyOptions: Unable to read available frequencies for core %u, file %s, reason: %s\n", coreid, path_to_cpufreq, strerror(errno));
			return {};
		}

		// list returned by this function is expected to be in ascending order, we may get descending
		std::sort(frequencies.begin(), frequencies.end());

		return frequencies;
	}

	Frequency getFrequency(Core coreid) {
		char path_to_cpufreq[FREQ_PATH_MAX_LENGTH];
		sprintf(path_to_cpufreq, FREQ_PATH_STRING, coreid, FREQ_CUR_STRING);

		std::ifstream file(path_to_cpufreq, std::ios::binary);

		auto freqs = getFrequencyOptions(coreid);

		if(!file) {
			FREQ_DBG("hw::getFrequency: Unable to open frequency file %s for reading, reason: %s\n", path_to_cpufreq, strerror(errno));
			return freqs[0];
		}

		int freq_in_khz = 0;
		file >> freq_in_khz;
		if(!file) {
			FREQ_DBG("hw::getFrequency: Unable to read frequency from file %s, reason: %s\n", path_to_cpufreq, strerror(errno));
			return freqs[0];
		}

		auto read_freq = Frequency::kHz(freq_in_khz);

		// the frequency we get from FREQ_CUR_STRING might not exactly be in the list for the given core
		// the rest of the system expects it to be, so we fix that here by choosing the closest
		auto it = std::find_if(freqs.cbegin(), freqs.cend(), [&](Frequency f) { return f >= read_freq; });
		if(it == freqs.cend()) {
			FREQ_DBG("hw::getFrequency: Unexpectedly high frequency in file %s: %s\n", path_to_cpufreq, toString(read_freq).c_str());
			return freqs.back();
		}
		if(it == freqs.cbegin()) return *it;
		auto smaller = *(it-1);
		auto larger = *it;
		if(read_freq - smaller > larger - read_freq) return larger;
		else return smaller;
	}

	namespace {
		bool setFrequencyPath(const char* path, Frequency freq) {
			std::ofstream file(path, std::ios::binary);

			if(!file) {
				FREQ_DBG("hw::setFrequency: Unable to open frequency file %s for writing, reason: %s\n", path, strerror(errno));
				return false;
			}

			file << freq.tokHz();

			if(!file) {
				FREQ_DBG("hw::setFrequency: Unable to write frequency to file %s, reason: %s\n", path, strerror(errno));
				return false;
			}

			return true;
		}

		bool setFrequencyInternal(Core coreid, Frequency minFreq, Frequency maxFreq) {

			// We are not allowed to write a lower min than max frequency, or a higher max than min
			// we could read it out first, or try to do some internal caching to know in which direction we are going
			// but simply writing it 3 times instead of twice reliably works

			bool success = true;
			char path_to_cpufreq[FREQ_PATH_MAX_LENGTH];
			sprintf(path_to_cpufreq, FREQ_PATH_STRING, coreid, FREQ_MIN_STRING);
			success &= setFrequencyPath(path_to_cpufreq, minFreq);
			sprintf(path_to_cpufreq, FREQ_PATH_STRING, coreid, FREQ_MAX_STRING);
			success &= setFrequencyPath(path_to_cpufreq, maxFreq);
			sprintf(path_to_cpufreq, FREQ_PATH_STRING, coreid, FREQ_MIN_STRING);
			success &= setFrequencyPath(path_to_cpufreq, minFreq);

			return success;
		}
	}

	bool setFrequency(Core coreid, Frequency freq) {
		return setFrequencyInternal(coreid, freq, freq);
	}

	void resetFrequency(Core core) {
		auto freqs = getFrequencyOptions(core);
		if(!freqs.empty()) setFrequencyInternal(core, freqs.front(), freqs.back());
	}

	namespace testing {
		// internally used to unit test caching
		thread_local int getFrequencyOptions_num_file_accesses = 0;
	}

} // end of namespace hw
#endif // USE_LINUX_CPUFREQ
} // end of namespace runtime
} // end of namespace allscale
