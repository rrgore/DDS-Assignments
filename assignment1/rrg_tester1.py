import Interface1 as Module
import os
from time import time

RATINGS_TABLE_NAME = 'ratings'
INPUT_FILE_PATH_NAME = 'test_data1.txt'
RANGE_TABLE_PREFIX = 'range_ratings_part'
RROBIN_TABLE_PREFIX = 'round_robin_ratings_part'
INPUT_FILE_PATH_NAME2 = '.\\..\\ml-10M100K\\ratings.dat'

def testCase1():
    print("---- Test case 1 ----")
    conn = Module.getOpenConnection(dbname='cse512')
    Module.loadRatings(RATINGS_TABLE_NAME, INPUT_FILE_PATH_NAME, conn)
    Module.rangePartition(RATINGS_TABLE_NAME, 3, conn)
    Module.rangeInsert(RATINGS_TABLE_NAME, 1, 589, 4.2, conn)
    Module.rangeInsert(RATINGS_TABLE_NAME, 1, 589, 1.2, conn)
    Module.rangeInsert(RATINGS_TABLE_NAME, 1, 589, 3.2, conn)
    Module.roundRobinPartition(RATINGS_TABLE_NAME, 3, conn)
    Module.roundRobinInsert(RATINGS_TABLE_NAME, 1, 589, 4.2, conn)
    Module.roundRobinInsert(RATINGS_TABLE_NAME, 1, 589, 1.2, conn)
    Module.roundRobinInsert(RATINGS_TABLE_NAME, 1, 589, 3.2, conn)
    Module.rangeQuery(1, 5, conn, "./rangeResult.txt")
    Module.pointQuery(5, conn, "./pointResult.txt")
    Module.pointQuery(1.5, conn, "./pointResult.txt")
    Module.pointQuery(3.2, conn, "./pointResult.txt")
    cleanUp(3, 3)


def testCase2():
    print("---- Test case 2 ----")
    try:
        conn = Module.getOpenConnection(dbname='cse512')
        # Path for ratings.dat
        Module.loadRatings(RATINGS_TABLE_NAME, INPUT_FILE_PATH_NAME2, conn)
        Module.rangePartition(RATINGS_TABLE_NAME, 15, conn)
        range_insert_list = [
            (10, 771, 4.6), (26, 361, 2.3), (91, 105, 3.1)
        ]
        for (u, m, r) in range_insert_list:
            Module.rangeInsert(RATINGS_TABLE_NAME, u, m, r, conn)
        Module.rangeQuery(1.7, 4.4, conn, "./rangeResult.txt")
        count = 0
        with open('rangeResult.txt') as f:
            count = len(f.readlines())
        if count != 7272777:
            print("Test failed! Count is {}, must be 7272777".format(count))
            return
        Module.pointQuery(2.3, conn, "./pointResult.txt")
        with open('pointResult.txt') as f:
            count = len(f.readlines())
        if count != 2:
            print("Test failed! Count is {0}, must be 2".format(count))
            return
        Module.deleteTables(RATINGS_TABLE_NAME, conn)
        print("Test passed!")
    except Exception as ex:
        print("Test failed!")
        print(ex)


def testCase3():
    print("---- Test case 3 ----")
    conn = Module.getOpenConnection(dbname='cse512')
    # Path for ratings.dat
    Module.loadRatings(RATINGS_TABLE_NAME, INPUT_FILE_PATH_NAME, conn)
    Module.roundRobinPartition(RATINGS_TABLE_NAME, 4, conn)
    rrobin_insert_list = [
        (10, 771, 4.6), (26, 361, 2.3), (91, 105, 3.1)
    ]
    for (u, m, r) in rrobin_insert_list:
        Module.roundRobinInsert(RATINGS_TABLE_NAME, u, m, r, conn)
    Module.rangeQuery(1.7, 4.4, conn, "./rangeResult.txt")
    Module.pointQuery(3.5, conn, "./pointResult.txt")
    cleanUp( 0, 4 )

# Edge cases
def testCase4():
    print("---- Test case 4 ----")
    try:
        conn = Module.getOpenConnection(dbname='cse512')
        # Path for ratings.dat
        Module.loadRatings(RATINGS_TABLE_NAME, INPUT_FILE_PATH_NAME, conn)
        Module.rangeInsert(RATINGS_TABLE_NAME, 9, 310, 4.0, conn)
        Module.roundRobinInsert(RATINGS_TABLE_NAME, 3, 102, 2.5, conn)
        Module.rangeQuery(1.7, 4.4, conn, "./rangeResult.txt")
        Module.pointQuery(3.5, conn, "./pointResult.txt")
        Module.deleteTables(RATINGS_TABLE_NAME, conn)
    except Exception as ex:
        print("Test failed!")
        print(ex)

def testCase5():
    print("---- Test case 5 ----")
    try:
        conn = Module.getOpenConnection(dbname='cse512')
        # Path for ratings.dat
        Module.loadRatings(RATINGS_TABLE_NAME, INPUT_FILE_PATH_NAME, conn)
        Module.rangePartition(RATINGS_TABLE_NAME, 0, conn)
        Module.rangeInsert(RATINGS_TABLE_NAME, 9, 310, 4.0, conn)
        Module.rangeQuery(1.7, 4.4, conn, "./rangeResult.txt")
        Module.pointQuery(3.5, conn, "./pointResult.txt")
        Module.deleteTables(RATINGS_TABLE_NAME, conn)
    except Exception as ex:
        print("Test failed!")
        print(ex)


def cleanUp( num_range=15, num_rrobin=3 ):
    # Cleanup the fragments. Then ratings_metadata, then ratings
    # range_frag_name = [RANGE_TABLE_PREFIX + str(i) for i in range(num_range)]
    # rrobin_frag_name = [RROBIN_TABLE_PREFIX + str(i) for i in range(num_rrobin)]
    conn = Module.getOpenConnection(dbname='cse512')
    # for i in range(num_range):
    #     Module.deleteTables(range_frag_name[i], conn)
    # for i in range(num_rrobin):
    #     Module.deleteTables(rrobin_frag_name[i], conn)
    # Module.deleteTables('ratings_metadata', conn)
    # Module.deleteTables(RATINGS_TABLE_NAME, conn)
    Module.deleteTables('ALL', conn)


# Test driver
if __name__ == '__main__':    
    # testCase1()
    # testCase2()
    # testCase3()
    # testCase4()
    # testCase5()
    cleanUp()
    
