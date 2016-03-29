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
		
		System.out.println("SCAN " + output.render());
		
		op.setOutput(output);
	}

	public void visit(Project op) {
		
		// get the inputs from the 
		Relation input = op.getInput().getOutput();
		Relation output = new Relation(input.getTupleCount());
		
		// add the attributes in the project
		op.getAttributes().forEach(attr -> output.addAttribute(new Attribute(attrs.get(attr.getName()))));
		
		System.out.println("PROJECT " + output.render());
		
		// set output for the project
		op.setOutput(output);
	}
	
	public void visit(Select op) {
		
		Predicate p = op.getPredicate(); // the predicate
		Attribute left = new Attribute(attrs.get(p.getLeftAttribute().getName())); // left attr != null
		
		// the new attributes from select predicate with new value counts
		Attribute output_left_attr = null;
		
		// get the input relation, which is the output of the input operator tree
		Relation input = op.getInput().getOutput();
		Relation output;
		
		if(p.equalsValue()) {
			// attr = val
			output = new Relation(input.getTupleCount()/left.getValueCount());
			output_left_attr = new Attribute(left.getName(), 1);
			
			// add the attributes from the original relation except the selection attrs, A			
			input.getAttributes()
				 .stream()
				 .filter(attr -> !attr.equals(left))
				 .forEach(attr -> output.addAttribute(new Attribute(attr)));
			
			// add left attr always
			output.addAttribute(output_left_attr);
			attrs.put(output_left_attr.getName(), output_left_attr); // add to local record
		} else {
			// attr = attr
			Attribute right = new Attribute(attrs.get(p.getRightAttribute().getName())); // right != null
			output = new Relation(input.getTupleCount()/Math.max(left.getValueCount(), right.getValueCount()));
			
			int size = Math.min(left.getValueCount(), right.getValueCount());
			output_left_attr = new Attribute(left.getName(), size);
			Attribute output_right_attr = new Attribute(right.getName(), size);
			
			// add the attributes from the original relation except the selection attrs, A			
			input.getAttributes()
				 .stream()
				 .filter(attr -> !attr.equals(left) && !attr.equals(right))
				 .forEach(attr -> output.addAttribute(new Attribute(attr)));
			
			// add left attr always
			output.addAttribute(output_left_attr);
			attrs.put(output_left_attr.getName(), output_left_attr); // add to local record
			
			output.addAttribute(output_right_attr);
			attrs.put(output_right_attr.getName(), output_right_attr); // add to local record
		}
		
		
		System.out.println("SELECT " + output.render());
		
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
		left.getAttributes().forEach(attr -> output.addAttribute(new Attribute(attr)));
		
		// add attributes from right
		right.getAttributes().forEach(attr -> output.addAttribute(new Attribute(attr)));
		
		System.out.println("PRODUCT " + output.render());
		
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
