DROP DATABASE IF EXISTS TouchstoneTPCH5;
CREATE DATABASE TouchstoneTPCH5;
USE TouchstoneTPCH5;
SET GLOBAL LOCAL_INFILE=1;
-- SCCSID:     @(#)DSS.DDL	2.1.8.1

CREATE TABLE `REGION` (
  `R_REGIONKEY` INT(11) NOT NULL,
  `R_NAME` CHAR(25) NOT NULL,
  `R_COMMENT` VARCHAR(152) DEFAULT NULL,
  PRIMARY KEY (`R_REGIONKEY`)
) ENGINE=INNODB DEFAULT CHARSET=UTF8MB4  ;

CREATE TABLE `NATION` (
  `N_NATIONKEY` INT(11) NOT NULL,
  `N_NAME` CHAR(25) NOT NULL,
  `N_REGIONKEY` INT(11) NOT NULL,
  `N_COMMENT` VARCHAR(152) DEFAULT NULL,
  PRIMARY KEY (`N_NATIONKEY`),
  KEY `NATION_FK1` (`N_REGIONKEY`),
  CONSTRAINT `NATION_IBFK_1` FOREIGN KEY (`N_REGIONKEY`) REFERENCES `REGION` (`R_REGIONKEY`)
) ENGINE=INNODB DEFAULT CHARSET=UTF8MB4  ;


CREATE TABLE `PART` (
  `P_PARTKEY` INT(11) NOT NULL,
  `P_NAME` VARCHAR(55) NOT NULL,
  `P_MFGR` CHAR(25) NOT NULL,
  `P_BRAND` CHAR(10) NOT NULL,
  `P_TYPE` VARCHAR(25) NOT NULL,
  `P_SIZE` INT(11) NOT NULL,
  `P_CONTAINER` CHAR(10) NOT NULL,
  `P_RETAILPRICE` DECIMAL(15,2) NOT NULL,
  `P_COMMENT` VARCHAR(23) NOT NULL,
  PRIMARY KEY (`P_PARTKEY`)
) ENGINE=INNODB DEFAULT CHARSET=UTF8MB4  ;


CREATE TABLE `SUPPLIER` (
  `S_SUPPKEY` INT(11) NOT NULL,
  `S_NAME` CHAR(25) NOT NULL,
  `S_ADDRESS` VARCHAR(40) NOT NULL,
  `S_NATIONKEY` INT(11) NOT NULL,
  `S_PHONE` CHAR(15) NOT NULL,
  `S_ACCTBAL` DECIMAL(15,2) NOT NULL,
  `S_COMMENT` VARCHAR(101) NOT NULL,
  PRIMARY KEY (`S_SUPPKEY`),
  KEY `SUPPLIER_FK1` (`S_NATIONKEY`),
  CONSTRAINT `SUPPLIER_IBFK_1` FOREIGN KEY (`S_NATIONKEY`) REFERENCES `NATION` (`N_NATIONKEY`)
) ENGINE=INNODB DEFAULT CHARSET=UTF8MB4  ;

CREATE TABLE `PARTSUPP` (
  `PS_PARTKEY` INT(11) NOT NULL,
  `PS_SUPPKEY` INT(11) NOT NULL,
  `PS_AVAILQTY` INT(11) NOT NULL,
  `PS_SUPPLYCOST` DECIMAL(15,2) NOT NULL,
  `PS_COMMENT` VARCHAR(199) NOT NULL,
  `PS_KEY` INT(11) NOT NULL,
  PRIMARY KEY (`PS_PARTKEY`,`PS_SUPPKEY`,`PS_KEY`),
  KEY `PARTSUPP_FK1` (`PS_SUPPKEY`),
  CONSTRAINT `PARTSUPP_IBFK_1` FOREIGN KEY (`PS_SUPPKEY`) REFERENCES `SUPPLIER` (`S_SUPPKEY`),
  CONSTRAINT `PARTSUPP_IBFK_2` FOREIGN KEY (`PS_PARTKEY`) REFERENCES `PART` (`P_PARTKEY`)
) ENGINE=INNODB DEFAULT CHARSET=UTF8MB4  ;

CREATE TABLE `CUSTOMER` (
  `C_CUSTKEY` INT(11) NOT NULL,
  `C_NAME` VARCHAR(25) NOT NULL,
  `C_ADDRESS` VARCHAR(40) NOT NULL,
  `C_NATIONKEY` INT(11) NOT NULL,
  `C_PHONE` CHAR(15) NOT NULL,
  `C_ACCTBAL` DECIMAL(15,2) NOT NULL,
  `C_MKTSEGMENT` CHAR(10) NOT NULL,
  `C_COMMENT` VARCHAR(117) NOT NULL,
  PRIMARY KEY (`C_CUSTKEY`),
  KEY `CUSTOMER_FK1` (`C_NATIONKEY`),
  CONSTRAINT `CUSTOMER_IBFK_1` FOREIGN KEY (`C_NATIONKEY`) REFERENCES `NATION` (`N_NATIONKEY`)
) ENGINE=INNODB DEFAULT CHARSET=UTF8MB4  ;

CREATE TABLE `ORDERS` (
  `O_ORDERKEY` INT(11) NOT NULL,
  `O_CUSTKEY` INT(11) NOT NULL,
  `O_ORDERSTATUS` CHAR(1) NOT NULL,
  `O_TOTALPRICE` DECIMAL(15,2) NOT NULL,
  `O_ORDERDATE` DATE NOT NULL,
  `O_ORDERPRIORITY` CHAR(15) NOT NULL,
  `O_CLERK` CHAR(15) NOT NULL,
  `O_SHIPPRIORITY` INT(11) NOT NULL,
  `O_COMMENT` VARCHAR(79) NOT NULL,
  PRIMARY KEY (`O_ORDERKEY`),
  KEY `ORDERS_FK1` (`O_CUSTKEY`),
  CONSTRAINT `ORDERS_IBFK_1` FOREIGN KEY (`O_CUSTKEY`) REFERENCES `CUSTOMER` (`C_CUSTKEY`)
) ENGINE=INNODB DEFAULT CHARSET=UTF8MB4  ;

CREATE TABLE `LINEITEM` (
  `L_ORDERKEY` INT(11) NOT NULL,
  `L_PARTKEY` INT(11) NOT NULL,
  `L_SUPPKEY` INT(11) NOT NULL,
  `L_LINENUMBER` INT(11) NOT NULL,
  `L_QUANTITY` DECIMAL(15,2) NOT NULL,
  `L_EXTENDEDPRICE` DECIMAL(15,2) NOT NULL,
  `L_DISCOUNT` DECIMAL(15,8) NOT NULL,
  `L_TAX` DECIMAL(15,2) NOT NULL,
  `L_RETURNFLAG` CHAR(1) NOT NULL,
  `L_LINESTATUS` CHAR(1) NOT NULL,
  `L_SHIPDATE` DATE NOT NULL,
  `L_COMMITDATE` DATE NOT NULL,
  `L_RECEIPTDATE` DATE NOT NULL,
  `L_SHIPINSTRUCT` CHAR(25) NOT NULL,
  `L_SHIPMODE` CHAR(10) NOT NULL,
  `L_COMMENT` VARCHAR(44) NOT NULL,
  PRIMARY KEY (`L_ORDERKEY`,`L_LINENUMBER`),
  KEY `LINEITEM_FK2` (`L_PARTKEY`,`L_SUPPKEY`),
  CONSTRAINT `LINEITEM_IBFK_1` FOREIGN KEY (`L_ORDERKEY`) REFERENCES `ORDERS` (`O_ORDERKEY`),
  CONSTRAINT `LINEITEM_IBFK_2` FOREIGN KEY (`L_PARTKEY`, `L_SUPPKEY`) REFERENCES `PARTSUPP` (`PS_PARTKEY`, `PS_SUPPKEY`)
) ENGINE=INNODB DEFAULT CHARSET=UTF8MB4  ;

-- SCCSID:     @(#)DSS.RI	2.1.8.1
-- TPCH BENCHMARK VERSION 8.0

USE TouchstoneTPCH5;

-- ALTER TABLE TPCH.REGION DROP PRIMARY KEY;
-- ALTER TABLE TPCH.NATION DROP PRIMARY KEY;
-- ALTER TABLE TPCH.PART DROP PRIMARY KEY;
-- ALTER TABLE TPCH.SUPPLIER DROP PRIMARY KEY;
-- ALTER TABLE TPCH.PARTSUPP DROP PRIMARY KEY;
-- ALTER TABLE TPCH.ORDERS DROP PRIMARY KEY;
-- ALTER TABLE TPCH.LINEITEM DROP PRIMARY KEY;
-- ALTER TABLE TPCH.CUSTOMER DROP PRIMARY KEY;


ALTER TABLE CUSTOMER RENAME TO customer;
ALTER TABLE LINEITEM RENAME TO lineitem;
ALTER TABLE NATION RENAME TO nation;
ALTER TABLE ORDERS RENAME TO orders;
ALTER TABLE PART RENAME TO part;
ALTER TABLE PARTSUPP RENAME TO partsupp;
ALTER TABLE REGION RENAME TO region;
ALTER TABLE SUPPLIER RENAME TO supplier;
USE TouchstoneTPCH5;
SET FOREIGN_KEY_CHECKS=0;
load data concurrent local infile 'tstpchdata/data5/customer.tbl' into table customer
fields terminated by ',' lines terminated by '\n';
load data concurrent local infile 'tstpchdata/data5/lineitem.tbl' into table lineitem
fields terminated by ',' lines terminated by '\n';
load data concurrent local infile 'tstpchdata/data5/nation.tbl' into table nation
fields terminated by ',' lines terminated by '\n';
load data concurrent local infile 'tstpchdata/data5/orders.tbl' into table orders
fields terminated by ',' lines terminated by '\n';
load data concurrent local infile 'tstpchdata/data5/partsupp.tbl' into table partsupp
fields terminated by ',' lines terminated by '\n';
load data concurrent local infile 'tstpchdata/data5/part.tbl' into table part
fields terminated by ',' lines terminated by '\n';
load data concurrent local infile 'tstpchdata/data5/region.tbl' into table region
fields terminated by ',' lines terminated by '\n';
load data concurrent local infile 'tstpchdata/data5/supplier.tbl' into table supplier
fields terminated by ',' lines terminated by '\n';
SET FOREIGN_KEY_CHECKS=1;

