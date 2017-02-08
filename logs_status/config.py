from collections import namedtuple

LogConfig = namedtuple('LogConfig', ['name', 'update_freq', 'dir_level'])

logs_config_table = []

logs_config_table.append(LogConfig('AdvertisementClickSummary', 'hourly', 2))
logs_config_table.append(LogConfig('AdvertisementShowSummary', 'hourly', 2))
logs_config_table.append(LogConfig('FreshUsers', 'hourly', 1))
logs_config_table.append(LogConfig('NewsClickSummary', 'hourly', 2))
logs_config_table.append(LogConfig('NewsImpressionSummary', 'hourly', 2))
logs_config_table.append(LogConfig('NewsLoggingSummary', 'hourly', 2))
logs_config_table.append(LogConfig('NewsReadDurationSummary', 'hourly', 2))
logs_config_table.append(LogConfig('NewsSearchSummary', 'hourly', 2))
logs_config_table.append(LogConfig('PhoneInfo', 'daily', 2))
logs_config_table.append(LogConfig('PredictLogSummary', 'hourly', 2))
logs_config_table.append(LogConfig('TrendingClickSummary', 'hourly', 2))
logs_config_table.append(LogConfig('TrendingImpressionSummary', 'hourly', 2))
logs_config_table.append(LogConfig('VideoStatisticsSummary', 'hourly', 2))
