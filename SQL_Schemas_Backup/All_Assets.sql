CREATE TABLE `All_Assets` (
  `ID` int DEFAULT NULL,
  `Data_Provider` varchar(255) NOT NULL,
  `Ticker` varchar(45) NOT NULL,
  `Historical_Data_Url` varchar(255) DEFAULT NULL,
  `Historical_Data_Req_Body` varchar(255) DEFAULT NULL,
  `Live_Data_Url` varchar(255) DEFAULT NULL,
  `Live_Data_Req_Body` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`Data_Provider`,`Ticker`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3

