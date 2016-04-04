package sjdb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Stack;

public class Optimiser {
	
	private Catalogue cat;
	
	public static final Estimator est = new Estimator();

	public Optimiser(Catalogue cat) {
		this.cat = cat;
	}

	public Operator optimise(Operator plan) {
	
		Sections s = new Sections();
		plan.accept(s);
		
		System.out.println("Attributes: " + s.allAttributes);
		System.out.println("Predicates: " + s.allPredicates);
		System.out.println("Scans: " + s.allScans);
		
		//Shakibs method
		List<Operator> operationBlocks = firstStage(s.allScans, s.allAttributes, s.allPredicates);
		
		//Sort operationBlocks on estimated size
		//Collections.sort(operationBlocks, new tupleCountComparator());
		
		Operator secondStageResult = buildProductOrJoin(operationBlocks, s.allPredicates);
	
		if (plan instanceof Project)
			return new Project(secondStageResult, ((Project) plan).getAttributes());
		else
			return secondStageResult;
	}

	
	public class tupleCountComparator implements Comparator<Operator> {
	    @Override
	    public int compare(Operator o1, Operator o2) {
	        return Integer.valueOf(o1.output.getTupleCount()).compareTo(Integer.valueOf(o2.output.getTupleCount()));
	    }
	}
	
	private static Operator checkOperatorForAttribute(List<Operator> oList, Attribute attr){
		Iterator<Operator> oIt = oList.iterator();
		while(oIt.hasNext()){
			Operator curOp = oIt.next();
			if (curOp.getOutput().getAttributes().contains(attr)){
				oIt.remove();
				return curOp;
			}
		}
		return null;
	}

	public static Operator buildProductOrJoin(List<Operator> ops, Set<Predicate> preds){
		
		Operator result = null;
		
		if (ops.size() == 1){
			result = ops.get(0);
			result.accept(est);
			return result;
		}
		
		Iterator<Predicate> it = preds.iterator();
		while(it.hasNext()){
			Predicate currentPred = it.next();
			Operator left = checkOperatorForAttribute(ops, currentPred.getLeftAttribute());
			Operator right = checkOperatorForAttribute(ops, currentPred.getRightAttribute());
			Operator newResult = null;
			
			if(left == null || right == null){
				newResult = new Select(left != null? left : right, currentPred);
				it.remove();
			}
			if(left != null && right != null){
				newResult = new Join(left, right, currentPred);
				it.remove();
			}
			
			newResult.accept(est);
			ops.add(newResult);
		}
		
		return ops.get(0);
	}

	public static Operator buildSelectsOnTop(Operator op, Set<Predicate> preds){
		List<Operator> oList = new ArrayList<>();
		oList.add(op);
			
		Iterator<Predicate> it = preds.iterator();
		while(it.hasNext()){
			
			Predicate currentPred = it.next();
			Operator last = oList.get(oList.size()-1);
			
			if(last.getOutput() == null) last.accept(est);
			
			// attr = val
			if (currentPred.equalsValue() && 
				last.getOutput().getAttributes().contains(currentPred.getLeftAttribute())) 
			{
				oList.add(new Select(last, new Predicate(new Attribute(currentPred.getLeftAttribute().getName(),
															   currentPred.getLeftAttribute().getValueCount()),
												 currentPred.getRightValue())));
				it.remove();
			}
				
			if (!currentPred.equalsValue() && 
				last.getOutput().getAttributes().contains(currentPred.getLeftAttribute()) &&
				last.getOutput().getAttributes().contains(currentPred.getRightAttribute()))
			{
				oList.add(new Select(last, new Predicate(new Attribute(currentPred.getLeftAttribute().getName(),
												  			   currentPred.getLeftAttribute().getValueCount()), 
												 new Attribute(currentPred.getRightAttribute().getName(),
														  	   currentPred.getRightAttribute().getValueCount()))));
				it.remove();
			}
		}
		
		return oList.get(oList.size()-1);
	}
	
	public static Operator buildProjectOnTop(Operator op, Set<Attribute> attrs){
		// see which attributes are to be projected and add a project on top of it 
		List<Attribute> applicable = new ArrayList<>();
		
		if(op.getOutput() == null) op.accept(est);
		
		Iterator<Attribute> attrIt = attrs.iterator();
		while(attrIt.hasNext()){
			Attribute attr = attrIt.next();
			if (op.getOutput().getAttributes().contains(attr)){
				applicable.add(attr);
				attrIt.remove();
			}
		}
		
		if (applicable.size() > 0) {
			Operator op2 = new Project(op, applicable);
			op2.accept(est);
			return op2;
		} else {
			return op;
		}
	}
	
	public static List<Operator> firstStage(Set<Scan> scans, Set<Attribute> attrs, Set<Predicate> predicates) {
		List<Operator> ops = new ArrayList<>(scans.size());
		
		for (Scan s: scans){
			Operator o = buildSelectsOnTop(s, predicates);
			ops.add(buildProjectOnTop(o, attrs));
		}
		
		return ops;
	}

	class Sections implements PlanVisitor {
		
		public Set<Attribute> allAttributes = new HashSet<>();
		public Set<Predicate> allPredicates = new HashSet<>();
		public Set<Scan> allScans = new HashSet<Scan>();
		

		@Override
		public void visit(Scan op) {
			allScans.add(new Scan((NamedRelation) op.getRelation()));			
		}

		@Override
		public void visit(Project op) {
			for(Attribute attr : op.getAttributes()) {
				allAttributes.add(new Attribute(attr.getName()));
			}
		}

		@Override
		public void visit(Select op) {
			if(op.getPredicate().equalsValue()) {
				allPredicates.add(new Predicate(op.getPredicate().getLeftAttribute(), op.getPredicate().getRightValue()));
				allAttributes.add(new Attribute(op.getPredicate().getLeftAttribute().getName()));
			} else {
				allPredicates.add(new Predicate(op.getPredicate().getLeftAttribute(), op.getPredicate().getRightAttribute()));
				allAttributes.add(new Attribute(op.getPredicate().getLeftAttribute().getName()));
				allAttributes.add(new Attribute(op.getPredicate().getRightAttribute().getName()));
			}			
		}

		@Override
		public void visit(Product op) {}

		@Override
		public void visit(Join op) {}		
	}

}
