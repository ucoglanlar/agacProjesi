#include <iostream>
#include <math.h>
#include "LeafNode.h"
#include "InternalNode.h"
#include "QueueAr.h"

using namespace std;

int insertSortedArray(int* array, int value, int numElements);
bool insertLeftSibling(int value);
bool insertRightSibling(int value);
void deleteKey(int value);

LeafNode::LeafNode(int LSize, InternalNode *p,
  BTreeNode *left, BTreeNode *right) : BTreeNode(LSize, p, left, right)
{
  values = new int[LSize];
}  // LeafNode()



int LeafNode::getMinimum()const
{
  if(count > 0)  // should always be the case
    return values[0];
  else
    return 0;

} // LeafNode::getMinimum()


LeafNode* LeafNode::insert(int value)
{
	
  // students must write this
  
  //Leaf is empty
  if(count == 0){
  
  	//Insert value into position one
  	values[0] = value;
  	count++;
  	
  	return NULL;
  	
  //Leaf is full, so SPLIT
  }else if(count == leafSize){
  	
  	//SPLIT
  	cout << "SPLIT" << endl;

	//checks if value can be inserted in left sibling
	bool canInsertLeft = true;

	//checks if value can be inserted in right sibling
	bool canInsertRight = true;
  	
	//make method
  	//Check if can borrow from left sibling
  	if(leftSibling != NULL){
  	
	canInsertLeft = insertLeftSibling(value);
  		
  	//make method
  	//Check if can borrow from right sibling
  	}else if(rightSibling != NULL){
  		
  	canInsertRight = insertRightSibling(value);	
  	
  	}
  	
  	
  	//Sean's Rule: Right side has more elements than the left side
  	//All leaves need to be AT LEAST half full
  	int leftSize = (leafSize / 2) + 1;
  	int rightSize = (leafSize + 1) - leftSize;
  	
  	cout << "Left #: " << leftSize << endl;
  	cout << "Right #: " << rightSize << endl;
  	
  	//Create temperary array and insert new number into it, then split
  	int* temp = new int[leafSize+1];
  	
  	//Copy elements from values array into temp
  	for(int i = 0; i < count; i++){
  		temp[i] = values[i];
  	}
  	
  	//Insert new value into temp array
  	insertSortedArray(temp, value, count+1);
  	
  	//Create new leaf node and set left sibling to be the current leaf node
  	LeafNode newLeaf(leafSize, parent, this, NULL);
  	
  	//Set right sibling of current leaf node to new leaf node
  	rightSibling = &newLeaf;
  	
  	//Set the now empty parts of current leaf node to 0
  	for(int i = leftSize; i < leafSize; i++){
  		values[i] = 0;
  	}
  	
  	//Reset count to number of elements after split
  	count = leftSize;
  	
  	
  	//Fill new leaf node with values greater than the current leaf node
  	//Sean's Rule
  	for(int i = leftSize; i < leafSize+1; i++){
  		newLeaf.insert(temp[i]);
  	}
  	
  	cout << "New Leaf Node: ";
  	
  	for(int i = 0 ; i < newLeaf.count; i++){
  		cout << newLeaf.values[i] << " ";
  	}
  	cout << endl;
  	
  	return NULL;
  	
  //Leaf has at least one value and at most leafSize - 1 values (0, leafSize)	
  }else{
  	
  	insertSortedArray(values, value, count);
  	
  	count++;
  	
  	return NULL;
  }
  
  return NULL; // to avoid warnings for now.
}  // LeafNode::insert()

void LeafNode::print(Queue <BTreeNode*> &queue)
{
  cout << "Leaf: ";
  for (int i = 0; i < count; i++)
    cout << values[i] << ' ';
  cout << endl;
} // LeafNode::print()

/*
*	Inserts value into an array in a sorted manner, from least to greatest, and returns the index position where the value was inserted
*	Modifies given array, with the new value inserted
*	@array: the int array to insert into
*	@value: the value to insert
*	@numElements: the current amount of elements in the array
*	@return: the position in the array the value was inserted
*/
int insertSortedArray(int* array, int value, int numElements){

	//Add new value in sorted order. Least to Greatest
  	for(int i = 0; i < numElements; i++){
  	
  		if(value < array[i]){
  			
  			//shift all elements after the i index to the right
  			for(int j = numElements; j > i; j--){
  				array[j] = array[j-1];
  			
  			}
  		 
  			//insert value	
  			array[i] = value;
  			break;
  		
  		//Inserted value is the largest value if at last iteration
  		}else if(i == numElements - 1){
  			
  			//Insert value after the last value inserted
  			array[numElements] = value;
  		}
  	
  	}
	
} // LeafNode::insertSortedArray() 




/*	Inserts value into left sibling if left sibling is not null
*	Method assumes the left sibling of current Leafnode is not null
*	returns true if value was inserted
*	returns false if left sibling's values[] is full
*
*/
bool LeafNode::insertLeftSibling(int value){



	//check if values[] is full in leftSibling
	if(leftSibling->getCount() == leafSize){
		
		//values[] in leftSibling is full, returns false
		return false;
	} else{
		
		//inserts value in leftSibling
		leftSibling->insert(value);

	
	}

	
	return true;
}

/*	Inserts value into right sibling if right sibling is not null
*	Method assumes the right sibling of current Leafnode is not null
*	returns true if value was inserted successfully
*	returns false if right sibling's values[] is full
*
*/
bool LeafNode::insertRightSibling(int value){



	//check if values[] is full in rightSibling
	if(rightSibling->getCount() == leafSize){
		
		//values[] in rightSibling is full, returns false
		return false;
	} else{
		
		//inserts value in rightSibling
		rightSibling->insert(value);

	
	}

	
	return true;
}

/* erases given key
*  intended to be used for adoption
*/
void LeafNode::deleteKey(int value){

	//index of value
	int index = -1;

	//find index of the value
	for(int i = 0; i < count; i++){

		if(values[i] == value){
			index = i;
		}
	}

	//index is not found, exits function
	if(index == -1){
		return;
	}

	//found index of value
	//shift values from index until the count to the left by 1
	for(int j = index; j < count; j++){

		//j is at the last index in values[], exits loop to avoid out of bounds error
		if( j == count - 1){
			break;
		}
		
		//shift elements to the left by 1
		values[j] = values[j + 1]; 
	}

	//decrement count by 1
	count = count - 1;
}




