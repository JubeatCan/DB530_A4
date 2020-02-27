
#ifndef BPLUS_C
#define BPLUS_C

#include "MyDB_INRecord.h"
#include "MyDB_BPlusTreeReaderWriter.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_PageListIteratorSelfSortingAlt.h"
#include "RecordComparator.h"
#include "MyDB_RecordIteratorAlt.h"

MyDB_BPlusTreeReaderWriter :: MyDB_BPlusTreeReaderWriter (string orderOnAttName, MyDB_TablePtr forMe, 
	MyDB_BufferManagerPtr myBuffer) : MyDB_TableReaderWriter (forMe, myBuffer) {

	// find the ordering attribute
	auto res = forMe->getSchema ()->getAttByName (orderOnAttName);

	// remember information about the ordering attribute
	orderingAttType = res.second;
	whichAttIsOrdering = res.first;

	// and the root location
	rootLocation = getTable ()->getRootLocation ();
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getSortedRangeIteratorAlt (MyDB_AttValPtr low, MyDB_AttValPtr high) {
    vector<MyDB_PageReaderWriter> rangePages;
    discoverPages(rootLocation, rangePages, low, high);

    MyDB_INRecordPtr lowPtr = getINRecord();
    MyDB_INRecordPtr highPtr = getINRecord();
    lowPtr -> setKey(low);
    highPtr -> setKey(high);
    MyDB_RecordPtr tempPtr = getEmptyRecord();
    MyDB_RecordPtr lhs = getEmptyRecord();
    MyDB_RecordPtr rhs = getEmptyRecord();

    function<bool()> lowBound = buildComparator(tempPtr, lowPtr);
    function<bool()> highBound = buildComparator(highPtr, tempPtr);
    function<bool()> comparator = buildComparator(lhs, rhs);

	return make_shared<MyDB_PageListIteratorSelfSortingAlt>(rangePages, lhs, rhs, comparator, tempPtr, lowBound, highBound, true);
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getRangeIteratorAlt (MyDB_AttValPtr low, MyDB_AttValPtr high) {
    vector<MyDB_PageReaderWriter> rangePages;
    discoverPages(rootLocation, rangePages, low, high);

    MyDB_INRecordPtr lowPtr = getINRecord();
    MyDB_INRecordPtr highPtr = getINRecord();
    lowPtr -> setKey(low);
    highPtr -> setKey(high);
    MyDB_RecordPtr tempPtr = getEmptyRecord();
    MyDB_RecordPtr lhs = getEmptyRecord();
    MyDB_RecordPtr rhs = getEmptyRecord();

    function<bool()> lowBound = buildComparator(tempPtr, lowPtr);
    function<bool()> highBound = buildComparator(highPtr, tempPtr);
    function<bool()> comparator = buildComparator(lhs, rhs);

    return make_shared<MyDB_PageListIteratorSelfSortingAlt>(rangePages, lhs, rhs, comparator, tempPtr, lowBound, highBound, false);
}


bool MyDB_BPlusTreeReaderWriter :: discoverPages (int whichPage, vector <MyDB_PageReaderWriter> &list, MyDB_AttValPtr low, MyDB_AttValPtr high) {
    queue<int> pageQ;
    pageQ.push(whichPage);

    while (!pageQ.empty()) {
        MyDB_PageReaderWriter curPage = this->operator[](pageQ.front());
        pageQ.pop();

        if (curPage.getType() == RegularPage) {
            list.push_back(curPage);
        }

        if (curPage.getType() != RegularPage) {
            MyDB_INRecordPtr lowPtr = getINRecord();
            MyDB_INRecordPtr highPtr = getINRecord();
            MyDB_INRecordPtr tempPtr = getINRecord();
            MyDB_RecordIteratorAltPtr tempRecIt = curPage.getIteratorAlt();

            lowPtr -> setKey(low);
            function<bool()> lowBound = buildComparator(tempPtr, lowPtr);
            highPtr -> setKey(high);
            function<bool()> highBound = buildComparator(highPtr, tempPtr);

            while (tempRecIt -> advance()) {
                tempRecIt -> getCurrent(tempPtr);
                if (lowBound()) {
                    continue;
                }
                if (highBound()) {
                    break;
                }

                pageQ.push(tempPtr -> getPtr());
            }
        }
    }

    return !list.empty();
}

void MyDB_BPlusTreeReaderWriter :: append (MyDB_RecordPtr appendMe) {
    if (rootLocation == -1) {
        MyDB_PageReaderWriter newRoot = this->operator[](0);
        MyDB_INRecordPtr newIN = getINRecord();
        MyDB_PageReaderWriter newLeaf = this->operator[](1);

        newRoot.clear();
        newIN->setPtr(1);
        newLeaf.append(appendMe);
        newLeaf.setType(RegularPage);
        newRoot.setType(DirectoryPage);
        newRoot.append(newIN);
        getTable() -> setRootLocation(0);
        rootLocation = 0;
    } else {
        MyDB_RecordPtr newRecInRoot = append(rootLocation, appendMe);

        if (newRecInRoot == nullptr) {
            return;
        } else {
            MyDB_PageReaderWriter newRoot = this->operator[](getTable()->lastPage() + 1);
            MyDB_INRecordPtr newInfIN = getINRecord();
            newRoot.setType(DirectoryPage);
            newRoot.append(newRecInRoot);
            newInfIN->setPtr(rootLocation);
            newRoot.append(newInfIN);

            rootLocation = getTable()->lastPage();
            getTable()->setRootLocation(rootLocation);
        }
    }
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: split (MyDB_PageReaderWriter splitMe, MyDB_RecordPtr andMe) {
    MyDB_PageReaderWriter newPage = this->operator[](getTable()->lastPage() + 1);
    MyDB_PageType currentType = splitMe.getType();
    MyDB_RecordPtr lhs, rhs;
    function<bool ()> comparator, insertionComp;
    if (currentType == RegularPage) {
        lhs = getEmptyRecord();
        rhs = getEmptyRecord();
    } else if (currentType == DirectoryPage) {
        lhs = getINRecord();
        rhs = getINRecord();
    }
    MyDB_INRecordPtr newIN = getINRecord();
    newPage.setType(currentType);
    comparator = buildComparator(lhs, rhs);
    RecordComparator comp (comparator, lhs, rhs);

    if (currentType == RegularPage) {
        splitMe.sortInPlace(comparator, lhs, rhs);
    }

    vector<MyDB_RecordPtr> listToSplit;
    MyDB_RecordIteratorAltPtr it = splitMe.getIteratorAlt();
    while (it->advance()) {
        MyDB_RecordPtr temp;
        if (currentType == RegularPage) {
            temp = getEmptyRecord();
        } else if (currentType = DirectoryPage) {
            temp = getINRecord();
        }
        it->getCurrent(temp);
        listToSplit.push_back(temp);
    }

    listToSplit.insert(lower_bound(listToSplit.begin(), listToSplit.end(), comp), andMe);
    splitMe.clear();

    MyDB_RecordPtr temp;
    int i;
    for (i = 0; i < (listToSplit.size()+1)/2; i++) {
        newPage.append(listToSplit[i]);
    }
    newIN->setPtr(getTable()->lastPage());
    newIN->setKey(getKey(listToSplit[i - 1]));
    for (; i < listToSplit.size(); i++) {
        splitMe.append(listToSplit[i]);
    }

	return newIN;
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: append (int whichPage, MyDB_RecordPtr appendMe) {
	return nullptr;
}

MyDB_INRecordPtr MyDB_BPlusTreeReaderWriter :: getINRecord () {
	return make_shared <MyDB_INRecord> (orderingAttType->createAttMax ());
}

void MyDB_BPlusTreeReaderWriter :: printTree () {
}

MyDB_AttValPtr MyDB_BPlusTreeReaderWriter :: getKey (MyDB_RecordPtr fromMe) {

	// in this case, got an IN record
	if (fromMe->getSchema () == nullptr) 
		return fromMe->getAtt (0)->getCopy ();

	// in this case, got a data record
	else 
		return fromMe->getAtt (whichAttIsOrdering)->getCopy ();
}

function <bool ()>  MyDB_BPlusTreeReaderWriter :: buildComparator (MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {

	MyDB_AttValPtr lhAtt, rhAtt;

	// in this case, the LHS is an IN record
	if (lhs->getSchema () == nullptr) {
		lhAtt = lhs->getAtt (0);	

	// here, it is a regular data record
	} else {
		lhAtt = lhs->getAtt (whichAttIsOrdering);
	}

	// in this case, the LHS is an IN record
	if (rhs->getSchema () == nullptr) {
		rhAtt = rhs->getAtt (0);	

	// here, it is a regular data record
	} else {
		rhAtt = rhs->getAtt (whichAttIsOrdering);
	}
	
	// now, build the comparison lambda and return
	if (orderingAttType->promotableToInt ()) {
		return [lhAtt, rhAtt] {return lhAtt->toInt () < rhAtt->toInt ();};
	} else if (orderingAttType->promotableToDouble ()) {
		return [lhAtt, rhAtt] {return lhAtt->toDouble () < rhAtt->toDouble ();};
	} else if (orderingAttType->promotableToString ()) {
		return [lhAtt, rhAtt] {return lhAtt->toString () < rhAtt->toString ();};
	} else {
		cout << "This is bad... cannot do anything with the >.\n";
		exit (1);
	}
}


#endif
