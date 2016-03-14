package sjdb;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

public class Estimator implements PlanVisitor {


	public Estimator() {
		// empty constructor
	}

	/* 
	 * Create output relation on Scan operator
	 *
	 * Example implementation of visit method for Scan operators.
	 */
	public void visit(Scan op) {
		Relation input = op.getRelation();
		Relation output = new Relation(input.getTupleCount());
		
		Iterator<Attribute> iter = input.getAttributes().iterator();
		while (iter.hasNext()) {
			output.addAttribute(new Attribute(iter.next()));
		}
		
		op.setOutput(output);
	}

	public void visit(Project op) {
	}
	
	public void visit(Select op) {
	}
	
	public void visit(Product op) {
		
		// get output from two subtrees
		Relation left = op.getLeft().getOutput();
		Relation right = op.getRight().getOutput();
		
		// output of the product.c_tuple = left.c_tuple * right.c_tuple
		Relation output = new Relation(left.getTupleCount() * right.getTupleCount());
		
		// add attributes from left
		Iterator<Attribute> liter = left.getAttributes().iterator();
		while (liter.hasNext()) {
			output.addAttribute(new Attribute(liter.next()));
		}
		
		// add attributes from right
		Iterator<Attribute> riter = right.getAttributes().iterator();
		while (riter.hasNext()) {
			output.addAttribute(new Attribute(riter.next()));
		}
		
		// set the output of the product
		op.setOutput(output);
	}
	
	public void visit(Join op) {
	}
}
