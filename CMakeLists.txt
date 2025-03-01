cmake_minimum_required(VERSION 3.15)
project(A4)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "-Wall -g -O0")

include_directories(Main/BufferMgr/headers)
include_directories(Main/Catalog/headers)
include_directories(Main/DatabaseTable/headers)
include_directories(Main/Qunit)
include_directories(Main/Qunit/headers)
include_directories(Main/Record/headers)

add_executable(A4
        Main/BPlusTest/source/BPlusQUnit.cc
        Main/BufferMgr/headers/CheckLRU.h
        Main/BufferMgr/headers/MyDB_BufferManager.h
        Main/BufferMgr/headers/MyDB_Page.h
        Main/BufferMgr/headers/MyDB_PageHandle.h
        Main/BufferMgr/headers/PageCompare.h
        Main/BufferMgr/headers/TableCompare.h
        Main/BufferMgr/source/MyDB_BufferManager.cc
        Main/BufferMgr/source/MyDB_Page.cc
#        Main/BufferTest/source/BufferQUnit.cc
        Main/Catalog/headers/MyDB_AttType.h
        Main/Catalog/headers/MyDB_Catalog.h
        Main/Catalog/headers/MyDB_PageType.h
        Main/Catalog/headers/MyDB_Schema.h
        Main/Catalog/headers/MyDB_Table.h
        Main/Catalog/source/MyDB_Catalog.cc
        Main/Catalog/source/MyDB_Schema.cc
        Main/Catalog/source/MyDB_Table.cc
        Main/DatabaseTable/headers/IteratorComparator.h
        Main/DatabaseTable/headers/MyDB_BPlusTreeReaderWriter.h
        Main/DatabaseTable/headers/MyDB_PageListIteratorAlt.h
        Main/DatabaseTable/headers/MyDB_PageListIteratorSelfSortingAlt.h
        Main/DatabaseTable/headers/MyDB_PageReaderWriter.h
        Main/DatabaseTable/headers/MyDB_PageRecIterator.h
        Main/DatabaseTable/headers/MyDB_PageRecIteratorAlt.h
        Main/DatabaseTable/headers/MyDB_RecordIterator.h
        Main/DatabaseTable/headers/MyDB_RecordIteratorAlt.h
        Main/DatabaseTable/headers/MyDB_RunQueueIteratorAlt.h
        Main/DatabaseTable/headers/MyDB_TableReaderWriter.h
        Main/DatabaseTable/headers/MyDB_TableRecIterator.h
        Main/DatabaseTable/headers/MyDB_TableRecIteratorAlt.h
        Main/DatabaseTable/headers/RecordComparator.h
        Main/DatabaseTable/headers/Sorting.h
        Main/DatabaseTable/source/MyDB_BPlusTreeReaderWriter.cc
        Main/DatabaseTable/source/MyDB_PageListIteratorAlt.cc
        Main/DatabaseTable/source/MyDB_PageReaderWriter.cc
        Main/DatabaseTable/source/MyDB_PageRecIterator.cc
        Main/DatabaseTable/source/MyDB_PageRecIteratorAlt.cc
        Main/DatabaseTable/source/MyDB_RunQueueIteratorAlt.cc
        Main/DatabaseTable/source/MyDB_TableReaderWriter.cc
        Main/DatabaseTable/source/MyDB_TableRecIterator.cc
        Main/DatabaseTable/source/MyDB_TableRecIteratorAlt.cc
        Main/DatabaseTable/source/Sorting.cc
        Main/Qunit/headers/QUnit.h
        Main/Record/headers/MyDB_AttVal.h
        Main/Record/headers/MyDB_INRecord.h
        Main/Record/headers/MyDB_Record.h
        Main/Record/source/MyDB_AttVal.cc
        Main/Record/source/MyDB_Record.cc
#        Main/RecordTest/source/RecordQUnit.cc
#        Main/SortTest/source/SortQUnit.cc
        )
