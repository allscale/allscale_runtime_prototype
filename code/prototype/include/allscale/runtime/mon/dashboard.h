#pragma once

namespace allscale {
namespace runtime {
namespace mon {

	constexpr const char* DEFAULT_DASHBOARD_IP = "127.0.0.1";

	/**
	 * The default port utilized to connect to the dashboard.
	 */
	constexpr int DEFAULT_DASHBOARD_PORT = 1337;

	constexpr const char* ENVVAR_DASHBOARD_IP = "ART_DASHBOARD_IP";

	constexpr const char* ENVVAR_DASHBOARD_PORT = "ART_DASHBOARD_PORT";

} // end of namespace mon
} // end of namespace runtime
} // end of namespace allscale
