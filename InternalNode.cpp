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
  
  
  return NULL; // to avoid warnings for now.
} // InternalNode::insert()

void InternalNode::insert(BTreeNode *oldRoot, BTreeNode *node2)
{ // Node must be the root, and node1
  // students must write this
  
  
} // InternalNode::insert()

void InternalNode::insert(BTreeNode *newNode) // from a sibling
{
  // students may write this
  
  
  
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
		
		//check if value is larger than the largest key
//		int currentSize = start->getCount();

//		InternalNode* ip = (InternalNode*)start;
		int currentSize = ip->getCount(); 		
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

	

}

