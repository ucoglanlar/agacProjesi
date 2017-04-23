#include <iostream>
#include "InternalNode.h"
#include <math.h>

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
  	//cout << "After minimum" << endl;
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
  	
  	//cout << "New node: " << newNode << endl;
  	//cout << "Children: ";
  	
  	//try to update key if already a child
  	for(int i = 0; i < count; i++){
  	
  		//cout << children[i] << " ";
  	
  		if(newNode == children[i]){
  			
  			//cout << endl << "Existing child" << endl;
  			keys[i] = minKey;
  			alreadyChild = true;
  			//cout << "Min key: " << minKey << endl;
  		}
  	}
  	
  	//If not a child, a new key
  	if(!alreadyChild){
  	
  		//cout << "A new key" << endl;
  		//cout << "Min key: " << minKey << endl;
  		//cout << "Count: " << count << endl;
  		
  		//If there is space
  		if(count < internalSize){
  			
  			//cout << "Space available" << endl;
  			 
  			insertSortedArray(children, keys, newNode, minKey, count);
  			
  			//cout << "Already NOT Child Min Key: " << minKey << endl;
  			
  			count++;
  			//cout << "Already NOT Child Count: " << count << endl;
  	
  			
  		
  		//Needs to split
  		}else{
  		
  			///////////////////////////
  			
  			//SPLIT
  			//cout << "SPLIT INTERNAL" << endl;

			/*//checks if value can be inserted in left sibling
			bool canInsertLeft = true;

			//checks if value can be inserted in right sibling
			bool canInsertRight = true;
  	
			//make method
  			//Check if can borrow from left sibling
  			if(leftSibling != NULL){
  	
  				cout << "Checking left" << endl;		
  		
				canInsertLeft = insertLeftSibling(value);
		
				if(canInsertLeft){
					return NULL;
				}
	
  		
  			//make method
  			//Check if can borrow from right sibling
  			}else if(rightSibling != NULL){
  		
  				cout << "Checking right" << endl;
  		
  				canInsertRight = insertRightSibling(value);	
  	
  				if(canInsertRight){
					return NULL;
				}
  	
  			}
  	
  			cout << "no adoption" << endl;
  			*/
  			
  			//Sean's Rule: Right side has more elements than the left side
  			//All leaves need to be AT LEAST half full
  			int leftSize = ceil(internalSize / 2.0);
  			//int rightSize = (internalSize + 1) - leftSize;
  	
  			//cout << "Left #: " << leftSize << endl;
  			//cout << "Right #: " << rightSize << endl;
  	
  			//Create temperary array and insert new number into it, then split
  			int* tempKeys = new int[internalSize+1];
  			BTreeNode** tempChildren = new BTreeNode*[internalSize+1];
  	
  			//Copy elements from values array into temp
  			for(int i = 0; i < count; i++){
  				tempKeys[i] = keys[i];
  				tempChildren[i] = children[i];
  			}
  	
  			//Insert new value into temp array
  			insertSortedArray(tempChildren, tempKeys, newNode, minKey, count);
  	
  			//Create new internal node pointer and set left sibling to be the current internal node
  			InternalNode* newInternal = new InternalNode(internalSize, leafSize, NULL, this, NULL);
  	
  			//cout << "New Leaf Created" << endl;
  	
  			//Set right sibling of current leaf node to new leaf node
  			rightSibling = newInternal;
  	
  			//Set left sibling of new leaf node to current leaf node
  			newInternal->setLeftSibling(this);
  	
  			
  			//Set the now empty parts of current internal node to 0
  			for(int i = leftSize; i < internalSize; i++){
  				keys[i] = 0;
  				children[i] = NULL;
  			}
  	
  			//Reset count to number of elements after split
  			count = leftSize;
  	
  	
  			//Fill new internal node with values greater than the current internal node
  			//Sean's Rule
  			for(int i = leftSize; i < internalSize+1; i++){
  				
  				//newInternal->insert(tempChildren[i]);
  				newInternal->keys[i-leftSize] = tempKeys[i];
  				newInternal->children[i-leftSize] = tempChildren[i];
  				newInternal->count++;
  				
  			}
  			
  			//Set children's parent to new Internal Node
  			for(int i = 0; i < newInternal->getCount(); i++){
  				
  				newInternal->children[i]->setParent(newInternal);
  				//cout << "To new parent: " << newInternal->children[i]->getMinimum() << endl;
  			}
  	
  			//cout << "Old Internal Node: ";
  	
  			for(int i = 0 ; i < count; i++){
  				//cout << keys[i] << " ";
  			}
  			//cout << endl;
  	
  			//cout << "New Internal Node: ";
  	
  			for(int i = 0 ; i < newInternal->count; i++){
  				//cout << newInternal->keys[i] << " ";
  			}
  			//cout << endl;
  	
  			//cout << "New Internal count: " << newInternal->getCount() << endl;
  	
  			//if it has a parent insert its minimum to parent
  			if(parent != NULL){
  				//cout << "Parent not null" << endl;
  				parent->insert(this);
  				//cout << "New Internal" << endl;
  				//parent->insert(newInternal);
  			}
  	
  			if(newInternal->parent == NULL){
  				newRoot = newInternal;
  			}
  	
  			for(int i = 0; i < newInternal->getCount(); i++){
  			
  				BTreeNode* child = newInternal->children[i];
  				if(i == 0){
  					child->setLeftSibling(NULL);
  				} else{
  				
  					child->setLeftSibling(newInternal->children[i - 1]);
					  				
  				}
  				
  				
  			}
  			
  			for(int i = 0; i < count; i++){
  				
  				BTreeNode* child = children[i];
  				if(i == 0){
  					child->setLeftSibling(NULL);
  				} else{
  				
  					child->setLeftSibling(children[i - 1]);
					  				
  				}
  			
  			}
  			
  			
			//cout << "INTERNAL SPLIT COUNT: " << getCount() << endl;
  			//cout << "NEW INTERNAL SPLIT COUNT: " << newInternal->getCount() << endl;
  			
  			
  			///////////////////////////
  			
  			
  			//newRoot = -1;
  			
  		
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
		
		//cout << "ip" << endl;
		
		int currentSize = ip->getCount(); 

		//if there is only one child in internal node, must traverse the child
		if(currentSize == 1){
			//cout << "==1" << endl;
			return find(value, ip->children[0]);
		}
		
		//cout << "ip2" << endl;
		
		//check if value is larger than the largest key		
		if(value >= ip->keys[currentSize - 1]){
			//cout << ">=" << endl;
			return find(value, ip->children[currentSize - 1]);
		}
 
 		//cout << "ip3" << endl;
 
		//traverse through the keys in internal node
		for(int i = 1; i < currentSize; i++){
			int temp = keys[i];

			//if temp key is greater than or equal to value, traverse child linked to left side of the key
			//keys in internal node are in increasing order
			if(value <= temp){
				//cout << "<=" << endl;
				return find(value, ip->children[i - 1]);
			}
		}
		
		cout << "ip4" << endl;

	}
	//start is the LeafNode
	else{
		//cout << "found: " << start->getMinimum() << endl;
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

