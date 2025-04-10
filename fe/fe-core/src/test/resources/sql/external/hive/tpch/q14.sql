[fragment statistics]
PLAN FRAGMENT 0(F05)
Output Exprs:30: expr
Input Partition: UNPARTITIONED
RESULT SINK

10:Project
|  output columns:
|  30 <-> 100.00 * [28: sum, DECIMAL128(38,4), true] / [29: sum, DECIMAL128(38,4), true]
|  cardinality: 1
|  column statistics:
|  * expr-->[0.0, 12942.348008385745, 0.0, 16.0, 1.0] ESTIMATE
|
9:AGGREGATE (merge finalize)
|  aggregate: sum[([28: sum, DECIMAL128(38,4), true]); args: DECIMAL128; result: DECIMAL128(38,4); args nullable: true; result nullable: true], sum[([29: sum, DECIMAL128(38,4), true]); args: DECIMAL128; result: DECIMAL128(38,4); args nullable: true; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * sum-->[0.0, 104949.5, 0.0, 16.0, 1.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 16.0, 1.0] ESTIMATE
|  * expr-->[0.0, 12942.348008385745, 0.0, 16.0, 1.0] ESTIMATE
|
8:EXCHANGE
distribution type: GATHER
cardinality: 1

PLAN FRAGMENT 1(F04)

Input Partition: HASH_PARTITIONED: 17: p_partkey
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 08

7:AGGREGATE (update serialize)
|  aggregate: sum[(if[(21: p_type LIKE 'PROMO%', [35: multiply, DECIMAL128(31,4), true], 0); args: BOOLEAN,DECIMAL128,DECIMAL128; result: DECIMAL128(31,4); args nullable: true; result nullable: true]); args: DECIMAL128; result: DECIMAL128(38,4); args nullable: true; result nullable: true], sum[([27: expr, DECIMAL128(31,4), true]); args: DECIMAL128; result: DECIMAL128(38,4); args nullable: true; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * sum-->[0.0, 104949.5, 0.0, 16.0, 1.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 16.0, 1.0] ESTIMATE
|
6:Project
|  output columns:
|  21 <-> [21: p_type, VARCHAR, true]
|  27 <-> clone([35: multiply, DECIMAL128(31,4), true])
|  35 <-> [35: multiply, DECIMAL128(31,4), true]
|  common expressions:
|  32 <-> [7: l_discount, DECIMAL64(15,2), true]
|  33 <-> 1 - [32: cast, DECIMAL64(16,2), true]
|  34 <-> cast([33: subtract, DECIMAL64(16,2), true] as DECIMAL128(16,2))
|  35 <-> [31: cast, DECIMAL128(15,2), true] * [34: cast, DECIMAL128(16,2), true]
|  31 <-> cast([6: l_extendedprice, DECIMAL64(15,2), true] as DECIMAL128(15,2))
|  cardinality: 6653886
|  column statistics:
|  * p_type-->[-Infinity, Infinity, 0.0, 25.0, 150.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 16.0, 3736520.0] ESTIMATE
|
5:HASH JOIN
|  join op: INNER JOIN (PARTITIONED)
|  equal join conjunct: [17: p_partkey, INT, true] = [2: l_partkey, INT, true]
|  build runtime filters:
|  - filter_id = 0, build_expr = (2: l_partkey), remote = true
|  output columns: 6, 7, 21
|  cardinality: 6653886
|  column statistics:
|  * l_partkey-->[1.0, 2.0E7, 0.0, 8.0, 6653885.645940593] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
|  * l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 6653885.645940593] ESTIMATE
|  * p_type-->[-Infinity, Infinity, 0.0, 25.0, 150.0] ESTIMATE
|  * case-->[0.0, 104949.5, 0.0, 16.0, 3736521.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 16.0, 3736520.0] ESTIMATE
|
|----4:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [2: l_partkey, INT, true]
|       cardinality: 6653886
|
1:EXCHANGE
distribution type: SHUFFLE
partition exprs: [17: p_partkey, INT, true]
cardinality: 20000000

PLAN FRAGMENT 2(F02)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 2: l_partkey
OutPut Exchange Id: 04

3:Project
|  output columns:
|  2 <-> [2: l_partkey, INT, true]
|  6 <-> [6: l_extendedprice, DECIMAL64(15,2), true]
|  7 <-> [7: l_discount, DECIMAL64(15,2), true]
|  cardinality: 6653886
|  column statistics:
|  * l_partkey-->[1.0, 2.0E7, 0.0, 8.0, 6653885.645940593] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
|  * l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|
2:HdfsScanNode
TABLE: lineitem
NON-PARTITION PREDICATES: 11: l_shipdate >= '1997-02-01', 11: l_shipdate < '1997-03-01'
MIN/MAX PREDICATES: 11: l_shipdate >= '1997-02-01', 11: l_shipdate < '1997-03-01'
partitions=1/1
avgRowSize=28.0
dataCacheOptions={populate: false}
cardinality: 6653886
column statistics:
* l_partkey-->[1.0, 2.0E7, 0.0, 8.0, 6653885.645940593] ESTIMATE
* l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
* l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
* l_shipdate-->[8.547264E8, 8.571456E8, 0.0, 4.0, 2526.0] ESTIMATE

PLAN FRAGMENT 3(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 17: p_partkey
OutPut Exchange Id: 01

0:HdfsScanNode
TABLE: part
NON-PARTITION PREDICATES: 17: p_partkey IS NOT NULL
partitions=1/1
avgRowSize=33.0
dataCacheOptions={populate: false}
cardinality: 20000000
probe runtime filters:
- filter_id = 0, probe_expr = (17: p_partkey)
column statistics:
* p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
* p_type-->[-Infinity, Infinity, 0.0, 25.0, 150.0] ESTIMATE
[end]