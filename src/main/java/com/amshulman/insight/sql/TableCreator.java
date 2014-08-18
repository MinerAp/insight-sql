package com.amshulman.insight.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TableCreator {

    public static void createBasicTables(ConnectionPool cp) throws SQLException {
        String createActionTable =
                "CREATE TABLE IF NOT EXISTS `actions` (" +
                "  `id` tinyint(3) unsigned NOT NULL AUTO_INCREMENT," +
                "  `name` varchar(32) NOT NULL," +
                "  PRIMARY KEY (`id`)," +
                "  UNIQUE KEY `name` (`name`)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";

        String createActorTable =
                "CREATE TABLE IF NOT EXISTS `actors` (" +
                "  `id` mediumint(8) unsigned NOT NULL AUTO_INCREMENT," +
                "  `name` varchar(32) NOT NULL," +
                "  `uuid` binary(16) DEFAULT NULL," +
                "  PRIMARY KEY (`id`)," +
                "  UNIQUE KEY `name` (`name`)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";

        String createMaterialTable =
                "CREATE TABLE IF NOT EXISTS `materials` (" +
                "  `id` smallint(5) unsigned NOT NULL AUTO_INCREMENT," +
                "  `namespace` varchar(64) NOT NULL," +
                "  `name` varchar(128) NOT NULL," +
                "  `subtype` smallint(5) unsigned NOT NULL," +
                "  PRIMARY KEY (`id`)," +
                "  UNIQUE KEY `name` (`name`,`subtype`,`namespace`)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";

        try (Connection conn = cp.getConnection();
                Statement stmt = conn.createStatement();) {
               conn.setAutoCommit(false);
               stmt.execute(createActionTable);
               stmt.execute(createActorTable);
               stmt.execute(createMaterialTable);
               conn.commit();
           }
    }

    public static void createWorldTables(ConnectionPool cp, String worldName) throws SQLException {
        String createBlockTable =
                "CREATE TABLE IF NOT EXISTS `%world%_blocks` (" +
                "  `id` int(10) unsigned NOT NULL AUTO_INCREMENT," +
                "  `datetime` datetime(3) NOT NULL," +
                "  `actionid` tinyint(3) unsigned NOT NULL," +
                "  `actorid` mediumint(8) unsigned NOT NULL," +
                "  `x` mediumint(9) NOT NULL," +
                "  `y` smallint(6) NOT NULL," +
                "  `z` mediumint(9) NOT NULL," +
                "  `blockid` smallint(5) unsigned NOT NULL," +
                "  `metadata` varbinary(8192) DEFAULT NULL," +
                "  PRIMARY KEY (`id`)," +
                "  KEY `datetime` (`datetime`)," +
                "  KEY `actionid` (`actionid`)," +
                "  KEY `actorid` (`actorid`)," +
                "  KEY `coordinates` (`x`,`z`,`y`)," +
                "  KEY `blockid` (`blockid`)," +
                "  CONSTRAINT `%world%_blocks_ibfk_1` FOREIGN KEY (`actionid`) REFERENCES `actions` (`id`)," +
                "  CONSTRAINT `%world%_blocks_ibfk_2` FOREIGN KEY (`actorid`) REFERENCES `actors` (`id`)," +
                "  CONSTRAINT `%world%_blocks_ibfk_3` FOREIGN KEY (`blockid`) REFERENCES `materials` (`id`)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";

        String createEntityTable =
                "CREATE TABLE IF NOT EXISTS `%world%_entities` (" +
                "  `id` int(10) unsigned NOT NULL AUTO_INCREMENT," +
                "  `datetime` datetime(3) NOT NULL," +
                "  `actionid` tinyint(3) unsigned NOT NULL," +
                "  `actorid` mediumint(8) unsigned NOT NULL," +
                "  `x` mediumint(9) NOT NULL," +
                "  `y` smallint(6) NOT NULL," +
                "  `z` mediumint(9) NOT NULL," +
                "  `acteeid` mediumint(8) unsigned NOT NULL," +
                "  `metadata` varbinary(8192) DEFAULT NULL," +
                "  PRIMARY KEY (`id`)," +
                "  KEY `datetime` (`datetime`)," +
                "  KEY `actionid` (`actionid`)," +
                "  KEY `actorid` (`actorid`)," +
                "  KEY `coordinates` (`x`,`z`,`y`)," +
                "  KEY `acteeid` (`acteeid`)," +
                "  CONSTRAINT `%world%_entities_ibfk_1` FOREIGN KEY (`actionid`) REFERENCES `actions` (`id`)," +
                "  CONSTRAINT `%world%_entities_ibfk_2` FOREIGN KEY (`actorid`) REFERENCES `actors` (`id`)," +
                "  CONSTRAINT `%world%_entities_ibfk_3` FOREIGN KEY (`acteeid`) REFERENCES `actors` (`id`)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";

        String createItemTable =
                "CREATE TABLE IF NOT EXISTS `%world%_items` (" +
                "  `id` int(10) unsigned NOT NULL AUTO_INCREMENT," +
                "  `datetime` datetime(3) NOT NULL," +
                "  `actionid` tinyint(3) unsigned NOT NULL," +
                "  `actorid` mediumint(8) unsigned NOT NULL," +
                "  `x` mediumint(9) NOT NULL," +
                "  `y` smallint(6) NOT NULL," +
                "  `z` mediumint(9) NOT NULL," +
                "  `itemid` smallint(5) unsigned NOT NULL," +
                "  `metadata` varbinary(8192) DEFAULT NULL," +
                "  PRIMARY KEY (`id`)," +
                "  KEY `datetime` (`datetime`)," +
                "  KEY `actionid` (`actionid`)," +
                "  KEY `actorid` (`actorid`)," +
                "  KEY `coordinates` (`x`,`z`,`y`)," +
                "  KEY `itemid` (`itemid`)," +
                "  CONSTRAINT `%world%_items_ibfk_1` FOREIGN KEY (`actionid`) REFERENCES `actions` (`id`)," +
                "  CONSTRAINT `%world%_items_ibfk_2` FOREIGN KEY (`actorid`) REFERENCES `actors` (`id`)," +
                "  CONSTRAINT `%world%_items_ibfk_3` FOREIGN KEY (`itemid`) REFERENCES `materials` (`id`)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";

        createBlockTable = createBlockTable.replaceAll("%world%", worldName);
        createEntityTable = createEntityTable.replaceAll("%world%", worldName);
        createItemTable = createItemTable.replaceAll("%world%", worldName);

        try (Connection conn = cp.getConnection();
             Statement stmt = conn.createStatement();) {
            conn.setAutoCommit(false);
            stmt.execute(createBlockTable);
            stmt.execute(createEntityTable);
            stmt.execute(createItemTable);
            conn.commit();
        }
    }
}
