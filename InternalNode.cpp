#include <iostream>
#include "InternalNode.h"

using namespace std;

InternalNode::InternalNode(int ISize, int LSize,
  InternalNode *p, BTreeNode *left, BTreeNode *right) :
  BTreeNode(LSize, p, left, right), internalSize(ISize)
{
  keys = new int[internalSize]; // keys[i] is the minimum of children[i]
  children = new BTreeNode* [ISize];
} // InternalNode::InternalNode()


int InternalNode::getMinimum()const
{
  if(count > 0)   // should always be the case
    return children[0]->getMinimum();
  else
    return 0;
} // InternalNode::getMinimum()


InternalNode* InternalNode::insert(int value)
{
  // students must write this
  
  
  /*if(parent == NULL){
  		return;
  }*/
  
  return NULL; // to avoid warnings for now.
} // InternalNode::insert()

void InternalNode::insert(BTreeNode *oldRoot, BTreeNode *node2)
{ // Node must be the root, and node1
  // students must write this
 
 	//cout << "Special insert" << endl;
 
  	keys[0] = oldRoot->getMinimum();
  	children[0] = oldRoot;
  	count++;
  	oldRoot->setParent(this);
  	
  	//cout << "Left done" << endl;
  	
  	//cout << "Node2 count: " << node2->getCount() << endl;
  	
  	keys[1] = node2->getMinimum();
  	cout << "After minimum" << endl;
  	children[1] = node2;
  	count++; 
  	node2->setParent(this);
  	
  	//cout << "Right done" << endl;
  
  
} // InternalNode::insert()

void InternalNode::insert(BTreeNode *newNode) // from a sibling
{
	// students may write this
	
	int minKey = newNode->getMinimum(); 
  	bool alreadyChild = false;
  	
  	cout << "New node: " << newNode << endl;
  	cout << "Children: ";
  	
  	//try to update key if already a child
  	for(int i = 0; i < count; i++){
  	
  		cout << children[i] << " ";
  	
  		if(newNode == children[i]){
  			
  			cout << endl << "Existing child" << endl;
  			keys[i] = minKey;
  			alreadyChild = true;
  			cout << "Min key: " << minKey << endl;
  		}
  	}
  	
  	if(!alreadyChild){
  	
  		cout << "A new key" << endl;
  	
  		//If there is space
  		if(count < internalSize){
  			
  			cout << "Space available" << endl;
  			 
  			insertSortedArray(children, keys, newNode, minKey, count);
  			
  			
  			
  			count++;
  		
  		//Needs to split
  		}else{
  		
  		
  		
  		}
  		
  	}
} // InternalNode::insert()


 

void InternalNode::print(Queue <BTreeNode*> &queue)
{
  int i;

  cout << "Internal: ";
  for (i = 0; i < count; i++)
    cout << keys[i] << ' ';
  cout << endl;

  for(i = 0; i < count; i++)
    queue.enqueue(children[i]);

} // InternalNode::print()


/* find returns a pointer to the correct LeafNode where the int value should be inserted
*  returns a BTreeNode pointer to the LeafNode
*
*/
BTreeNode* InternalNode::find(int value, BTreeNode* start){

	InternalNode* ip = dynamic_cast<InternalNode*>(start); 

	//if start is an internal Node
	if(ip != 0){

		int currentSize = ip->getCount(); 

		//if there is only one child in internal node, must traverse the child
		if(currentSize == 1){
			return find(value, children[0]);
		}
		
		//check if value is larger than the largest key		
		if(value >= ip->keys[currentSize - 1]){
			return find(value, children[currentSize - 1]);
		}
 
		//traverse through the keys in internal node
		for(int i = 1; i < currentSize; i++){
			int temp = keys[i];

			//if temp key is greater than or equal to value, traverse child linked to left side of the key
			//keys in internal node are in increasing order
			if(value <= temp){
				return find(value, children[i - 1]);
			}
		}

	}
	//start is the LeafNode
	else{

		return start;

	}

	return start;	

}

/*
*	Inserts value into an array in a sorted manner, from least to greatest, and returns the index position where the value was inserted
*	Modifies given array, with the new value inserted
*	@array: the BTreeNode* array to insert into
*	@keyArray: array of int keys
*	@pointer: the BTreeNode pointer to insert into array
*	@numElements: the current amount of elements in the array
*	@return: the position in the array the value was inserted
*/
int InternalNode::insertSortedArray(BTreeNode** array, int* keyArray, BTreeNode* pointer, int key, int numElements){

	//Add new value in sorted order. Least to Greatest
  	for(int i = 0; i < numElements; i++){
  	
  		if(key < keyArray[i]){
  			
  			//shift all elements after the i index to the right
  			for(int j = numElements; j > i; j--){
  				
  				keyArray[j] = keyArray[j-1];
  				array[j] = array[j-1];
  			
  			}
  		 
  			//insert value	
  			keyArray[i] = key;
  			array[i] = pointer;
  			
  			return i;
  		
  		//Inserted value is the largest value if at last iteration
  		}else if(i == numElements - 1){
  			
  			//Insert value after the last value inserted
  			keyArray[numElements] = key;
			array[numElements] = pointer;  			
  			
  			return numElements;
  		}
  	
  	}
  	
  	return -1;
	
} // InternalNode::insertSortedArray() 

