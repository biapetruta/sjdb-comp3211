package sjdb;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

public class Estimator implements PlanVisitor {
	
	// local record of attributes. Should have access to catalouge really....
	Map<String, Attribute> attrs = new HashMap<String, Attribute>();

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
			Attribute attr = iter.next();
			output.addAttribute(new Attribute(attr)); // add attribute to local record
			attrs.put(attr.getName(), new Attribute(attr));
		}
		
		op.setOutput(output);
	}

	public void visit(Project op) {
		
		// get the inputs from the 
		Relation input = op.getInput().getOutput();
		Relation output = new Relation(input.getTupleCount());
		
		// add the attributes in the project
		Iterator<Attribute> iter = op.getAttributes().iterator();
		while (iter.hasNext()) {
			// get the right attribute object from local record
			output.addAttribute(new Attribute(attrs.get(iter.next().getName())));
		}
		
		System.out.println("PROJECT " + output.getTupleCount());
		
		// set output for the project
		op.setOutput(output);
	}
	
	public void visit(Select op) {
		
		Predicate p = op.getPredicate(); // the predicate
		Attribute left = new Attribute(attrs.get(p.getLeftAttribute().getName())); // left attr != null
		Attribute right = null; // right can be == null
		
		// get the input relation, which is the output of the input operator tree
		Relation input = op.getInput().getOutput();
		Relation output;
		
		if(p.equalsValue()) {
			// attr = val
			output = new Relation(input.getTupleCount()/left.getValueCount());
		} else {
			// attr = attr
			right = new Attribute(attrs.get(p.getRightAttribute().getName()));
			output = new Relation(input.getTupleCount()/Math.max(left.getValueCount(), right.getValueCount()));
		}
		
		// add the attributes from the original relation except the section attrs, A, B
		Iterator<Attribute> iter = input.getAttributes().iterator();
		while (iter.hasNext()) {
			Attribute attr = iter.next();
			if (!attr.equals(left) && !attr.equals(right)) output.addAttribute(new Attribute(attr));
		}
		
		// the new attributes from select predicate with new value counts
		Attribute select_left = null;
		Attribute select_right = null;
		
		// attr = const
		if (p.equalsValue()) select_left = new Attribute(left.getName(), 1);
		else {
			// attr = attr
			int size = Math.min(left.getValueCount(), right.getValueCount());
			select_left = new Attribute(left.getName(), size);
			select_right = new Attribute(right.getName(), size);
		}
		
		// add left attr always
		output.addAttribute(select_left);
		attrs.put(select_left.getName(), select_left); // add to local record
		
		// add right attr if there is any
		if (select_right != null) {
			output.addAttribute(select_right);
			attrs.put(select_right.getName(), select_right); // add to local record
		}
		
		System.out.println("SELECT " + output.getTupleCount());
		
		// set the output to select
		op.setOutput(output);
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
		
		System.out.println("PRODUCT " + output.getTupleCount());
		
		// set the output of the product
		op.setOutput(output);
	}
	
	public void visit(Join op) {
		
		// get output from two subtrees
		Relation left = op.getLeft().getOutput();
		Relation right = op.getRight().getOutput();
		
		Predicate p = op.getPredicate(); // the predicate
		Attribute attr_left = new Attribute(attrs.get(p.getLeftAttribute().getName())); // left attr != null
		Attribute attr_right = new Attribute(attrs.get(p.getRightAttribute().getName())); // right attr != null
		
		// output of the join.c_tuple = T(R) * T(S) / max ( V(R,A) , V(S,B) )
		Relation output = new Relation(left.getTupleCount() * right.getTupleCount() / Math.max(attr_left.getValueCount(), attr_right.getValueCount()));
		
		// V(R_join, A) = V(R_join, A) = min ( V(R,A) , V(S,B) )
		int uniq_size = Math.min(attr_left.getValueCount(), attr_right.getValueCount());
		Attribute join_attr_left = new Attribute(attr_left.getName(), uniq_size);
		Attribute join_attr_right = new Attribute(attr_right.getName(), uniq_size);
		
		// add the attributes from left relation
		Iterator<Attribute> liter = left.getAttributes().iterator();
		while (liter.hasNext()) {
			Attribute attr = liter.next();
			if(!attr.equals(attr_left)) output.addAttribute(new Attribute(attr));
			else output.addAttribute(join_attr_left);
		}
		
		// add the attributes from the right relation
		Iterator<Attribute> riter = right.getAttributes().iterator();
		while (liter.hasNext()) {
			Attribute attr = riter.next();
			if(!attr.equals(attr_right)) output.addAttribute(new Attribute(attr));
			else output.addAttribute(join_attr_right);
		}
		
		// add the attributes to the local record
		attrs.put(join_attr_left.getName(), join_attr_left);
		attrs.put(join_attr_right.getName(), join_attr_right);
		
		op.setOutput(output);
	}
}
