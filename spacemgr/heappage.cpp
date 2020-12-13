#include <iostream>
#include <stdlib.h>
#include <memory.h>
#include <ctime>


#include "../include/heappage.h"
#include "../include/heapfile.h"
#include "../include/bufmgr.h"
#include "../include/db.h"

#define COMPACT_AFTER_DELETES


using namespace std;

//------------------------------------------------------------------
// Constructor of HeapPage
//
// Input     : Page ID
// Output    : None
//------------------------------------------------------------------


/* NOTES: a) We mark a record deleted by changing the length of the record to 0
 *        b) fillPtr points to the address of start of record closest to the start of data[]
 *
 */


void HeapPage::Init(PageID pageNo)
{
	this->pid = pageNo;
	this->prevPage = this->nextPage = INVALID_PAGE;
	this->freeSpace= HEAPPAGE_DATA_SIZE;
	this->numOfSlots = 0;
	this->fillPtr = HEAPPAGE_DATA_SIZE;

}

void HeapPage::SetNextPage(PageID pageNo)
{
	nextPage = pageNo;
}

void HeapPage::SetPrevPage(PageID pageNo)
{
	prevPage  = pageNo;
}

PageID HeapPage::GetNextPage()
{

	return nextPage;
}

PageID HeapPage::GetPrevPage()
{
	return prevPage;
}


//------------------------------------------------------------------
// HeapPage::InsertRecord
//
// Input     : Pointer to the record and the record's length 
// Output    : Record ID of the record inserted.
// Purpose   : Insert a record into the page
// Return    : OK if everything went OK, DONE if sufficient space 
//             does not exist
//------------------------------------------------------------------

Status HeapPage::InsertRecord(char *recPtr, int length, RecordID& rid)
{
	short oldSpace = freeSpace;

	freeSpace -= length * sizeof(char); //Free up space

	Slot * toFill = NULL;

	for(Slot* curr = slots; curr < slots + numOfSlots; ++curr)
		if(!curr->length){ //found an empty slot
			toFill = curr; break;
		}


	if(!toFill) //Free up space for another slot if we need a new one
		freeSpace -= sizeof(Slot);

	if(freeSpace < 0){ //We ran out of space, hence we failed and must return
		freeSpace = oldSpace; return DONE;
	}

	/*If we were unable to find a free slot, take the last one*/
	if(!toFill)
		toFill = slots + numOfSlots++;

	fillPtr -= length;     //Move up fill pointer in anticipation of insert

	//Record record info in slot and in Struct that will be output to caller
	toFill->length = length;
	toFill->offset = fillPtr;
	rid.pageNo = this->pid;
	rid.slotNo = toFill - slots;

	//Insert the actual record
	memcpy(data + fillPtr, recPtr, length * sizeof(char));
	return OK;

}

//------------------------------------------------------------------
// HeapPage::DeleteRecord 
//
// Input    : Record ID
// Output   : None
// Purpose  : Delete a record from the page
// Return   : OK if successful, FAIL otherwise  
//------------------------------------------------------------------ 

Status HeapPage::DeleteRecord(const RecordID& rid)
{
	short recLength = slots[rid.slotNo].length;
	if(rid.slotNo > numOfSlots || rid.slotNo < 0 || !recLength)
		return FAIL; //No such record exists


#ifdef COMPACT_AFTER_DELETES
	short recOffset = slots[rid.slotNo].offset;
	if(recOffset!= fillPtr){ //Otherwise you can just move up fillPtr

		char* recLocation = data + recOffset;


		memmove(data + fillPtr + recLength, data+fillPtr, recOffset - fillPtr);

		for(int slt = 0; slt < numOfSlots; ++slt){
			if(slots[slt].offset < recOffset)
				slots[slt].offset += recLength;
		}

	}
	fillPtr   += recLength;
	freeSpace += sizeof(char) * recLength;

#endif

	slots[rid.slotNo].length = 0; //mark record as deleted

	return OK;
}


//------------------------------------------------------------------
// HeapPage::FirstRecord
//
// Input    : None
// Output   : record id of the first record on a page
// Purpose  : To find the first record on a page
// Return   : OK if successful, DONE otherwise
//------------------------------------------------------------------

Status HeapPage::FirstRecord(RecordID& rid)
{
	for(int slt = 0; slt < numOfSlots; ++slt)
		if(slots[slt].length){
			rid.pageNo = this->pid;
			rid.slotNo = slt;
			return OK;
		}

	return DONE;
}


//------------------------------------------------------------------
// HeapPage::NextRecord
//
// Input    : ID of the current record
// Output   : ID of the next record
// Return   : Return DONE if no more records exist on the page; 
//            otherwise OK
//------------------------------------------------------------------

Status HeapPage::NextRecord (RecordID curRid, RecordID& nextRid)
{	if(curRid > numOfSlots || curRid < 0 || slots[curRid.slotNo].length == 0) //Does this current record even exist
		return FAIL;
	for(int slt = curRid.slotNo + 1; slt < numOfSlots; ++slt){ //Look for next record
		if(slots[slt].length){
			nextRid.pageNo = this->pid;
			nextRid.slotNo = slt;
			return OK;
		}
	}
	return DONE;
}

//------------------------------------------------------------------
// HeapPage::GetRecord
//
// Input    : Record ID
// Output   : Records length and a copy of the record itself
// Purpose  : To retrieve a _copy_ of a record with ID rid from a page
// Return   : OK if successful, FAIL otherwise
//------------------------------------------------------------------

Status HeapPage::GetRecord(RecordID rid, char *recPtr, int& length)
{
	//If the slot does not exist or is empty, notify caller of failure
	if(rid.slotNo >= numOfSlots || !slots[rid.slotNo].length)
		return FAIL;

	length = slots[rid.slotNo].length;

	memcpy(recPtr, data + slots[rid.slotNo].offset, length);

    return OK;
}


//------------------------------------------------------------------
// HeapPage::ReturnRecord
//
// Input    : Record ID
// Output   : pointer to the record, record's length
// Purpose  : To output a _pointer_ to the record
// Return   : OK if successful, FAIL otherwise
//------------------------------------------------------------------

Status HeapPage::ReturnRecord(RecordID rid, char*& recPtr, int& length)
{
	//If the slot does not exist or is empty, notify caller of failure
	if(rid.slotNo < 0 || rid.slotNo >= numOfSlots || !slots[rid.slotNo].length)
		return FAIL;

	length = slots[rid.slotNo].length;
	recPtr = data  + slots[rid.slotNo].offset;


    return OK;
}


//------------------------------------------------------------------
// HeapPage::AvailableSpace
//
// Input    : None
// Output   : None
// Purpose  : To return the amount of available space
// Return   : The amount of available space on the heap file page.
//------------------------------------------------------------------

int HeapPage::AvailableSpace(void)
{
	bool slotFree = false;

	for(int s = 0; s < numOfSlots; ++s)
		if(!slots[s].length){slotFree = true; break;} //find out if a slot is free

	return slotFree? freeSpace : freeSpace - sizeof(Slot);

}

//------------------------------------------------------------------
// HeapPage::IsEmpty
// 
// Input    : None
// Output   : None
// Purpose  : Check if there is any record in the page.
// Return   : true if the HeapPage is empty, and false otherwise.
//------------------------------------------------------------------

bool HeapPage::IsEmpty(void)
{
	if(!numOfSlots)
		return true;

	for(int slt = 0; slt < numOfSlots; ++slt)
		if(!slots[slt].length)
			return false;

	return true;
}


void HeapPage::CompactSlotDir()
{
  Slot * readPtr, *writePtr, *stopAt;
  readPtr = writePtr = slots;
  stopAt = slots + numOfSlots;

  while(readPtr < stopAt)
	  if(readPtr->length)  *writePtr++ = *readPtr++;
	  else ++readPtr;


  short slotsSaved = readPtr - writePtr;
  numOfSlots -= slotsSaved;
  freeSpace  += slotsSaved * sizeof(Slot);

}

int HeapPage::GetNumOfRecords()
{
	int numRecords = numOfSlots;

	for(int slt = 0; slt < numOfSlots; ++slt)
			if(!slots[slt].length)
				numRecords--;

  return numRecords;
}
