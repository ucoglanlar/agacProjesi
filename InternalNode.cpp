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
 
  	cout << "B" << endl;
	insert(oldRoot);
	cout << "C" << endl;
	insert(node2);
  
  
  
} // InternalNode::insert()

void InternalNode::insert(BTreeNode *newNode) // from a sibling
{
	// students may write this
	
	cout << "D" << endl;
	
	if(count < internalSize){
		
		newNode->setParent(this);
		cout << "\tE" << endl;
  		children[count] = newNode;
  		cout << "\tF" << endl;
  		keys[count] = newNode->getMinimum();
  		cout << "\tG" << endl;
  		
  		cout << "\tCount: " << count+1 << endl;
  		cout << "\tKey: " << keys[count] << endl;
  		
  		count++;
		
		
		
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


