CREATE TABLE `binance_assets` (
  `id` int NOT NULL AUTO_INCREMENT,
  `ticker` varchar(255) DEFAULT NULL,
  `historical_data_url` varchar(255) DEFAULT NULL,
  `live_data_url` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id_UNIQUE` (`id`),
  UNIQUE KEY `ticker_UNIQUE` (`ticker`),
  UNIQUE KEY `historical_data_url_UNIQUE` (`historical_data_url`),
  UNIQUE KEY `live_data_url_UNIQUE` (`live_data_url`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8mb3