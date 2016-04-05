package sjdb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

public class Optimiser {
	
	private Catalogue cat;
	
	private static final Estimator est = new Estimator();
	private static Operator top;

	public Optimiser(Catalogue cat) {
		this.cat = cat;
	}

	public Operator optimise(Operator plan) {
		
		top = plan;
	
		Sections s = new Sections();
		plan.accept(s);
		
		System.out.println("Attributes: " + s.allAttributes);
		System.out.println("Predicates: " + s.allPredicates);
		System.out.println("Scans: " + s.allScans);
		
		//Shakibs method
		List<Operator> operationBlocks = firstStage(s.allScans, s.allAttributes, s.allPredicates);
		
		List<Predicate> preds = new ArrayList<>();
		preds.addAll(s.allPredicates);
		
		List<List<Predicate>> permutedPredicates = generatePerm(preds);
		
		Operator cheapestPlan = null;
		Integer cheapestCost = Integer.MAX_VALUE;
		
		for (List<Predicate> p : permutedPredicates) {
			
			List<Operator> blocks = new ArrayList<>();
			blocks.addAll(operationBlocks);
			
			Operator aPlan = buildProductOrJoin(blocks, p);
			
			Integer i = est.getCost(aPlan);
			System.out.println("Found plan with cost: " + i);
			
			cheapestPlan = (i < cheapestCost) ? aPlan : cheapestPlan;
			cheapestCost = (i < cheapestCost) ? i : cheapestCost;
		}
		
		System.out.println("Cheapest cost = " + est.getCost(cheapestPlan));
		
		return cheapestPlan;
	}
	
	/**
	 * for each scan or relations at the leaves, process it as much as possible.
	 * the output operator should be 
	 * SCAN => [SELECT] x n => [PROJECT]_neededAttrs
	 * 
	 * @param scans the SCAN operators
	 * @param attrs the COMPLETE set of ATTRIBUTES needed from the scans
	 * @param predicates the COMPLETE set of PREDICATES needed from the scans
	 * @return the List of Operator BLOCKS in (SCAN => [SELECT] x n => [PROJECT]_neededAttrs) form,
	 * 			predicates will be mutated and truncated by removing the used ones
	 */
	public static List<Operator> firstStage(Set<Scan> scans, Set<Attribute> attrs, Set<Predicate> predicates) {
		
		// the block of resultant operators from each of the SCANs
		List<Operator> operatorBlocks = new ArrayList<>(scans.size());
		
		for (Scan s: scans){
			// to SCAN => [SELECT] x n
			Operator o = buildSelectsOnTop(s, predicates);
			// [SCAN] || [SELECT] x n => [PROJECT]
			operatorBlocks.add(buildProjectOnTop(o, attrs));
		}
		
		return operatorBlocks;
	}
	
	/**
	 * Iterate over the preds to see if any be applied on top of op.
	 * Then builds [multiple] SELECT Operators on top of op and returnds the topmost one.
	 * 
	 * @param op Operator to build SELECT operators on top of
	 * @param preds the set of PREDICATEs to choose from
	 * @return an Operator that is in the form of (op => [SELECT] x n) and
	 * 			the Set of PREDICATES is mutated and truncated by removing the used ones
	 */
	public static Operator buildSelectsOnTop(Operator op, Set<Predicate> preds){
		
		// The result
		Operator result = op;
		// the attributes available at this point, building SELECT doesn't remove any attributes
		List<Attribute> availableAttrs = result.getOutput().getAttributes();
			
		// Iterate over the PREDICATEs to see if any are applicable to the latest Operator in the list
		Iterator<Predicate> it = preds.iterator();
		while(it.hasNext()){
	
			Predicate currentPred = it.next();
			
			// If output of the latest Operator isn't set, set it
			if(result.getOutput() == null) result.accept(est);
			
			// attr = val and the ATTRIBUTE comes from the Operator's output relation
			if ((currentPred.equalsValue() && availableAttrs.contains(currentPred.getLeftAttribute())) || 
				(!currentPred.equalsValue() && availableAttrs.contains(currentPred.getLeftAttribute()) && availableAttrs.contains(currentPred.getRightAttribute()))) 
			{
				// add a new SELECT operator on top, note how the output isn't set
				result = new Select(result, currentPred);
				// remove the PREDICATE because it's been dealt with
				it.remove();
			}
			
		}
		
		return result;
	}
	
	/**
	 * Goes through the Set of needed ATTRIBUTEs and 
	 * checks which one of the current ATTRIBUTEs are needed.
	 * 
	 * If not ALL ATTRIBUTEs are needed, then puts a PROJECT on top.
	 * 
	 * @param op the Operator to build the project on Top of
	 * @param attrs the Set of ATTRIBUTES to check for
	 * @return an Operator in the form of [op] || [PROJECT => op]
	 */
	public static Operator buildProjectOnTop(Operator op, Set<Attribute> attrs){

		// if op doesn't have an output, fix that
		if(op.getOutput() == null) op.accept(est);
		
		// see which attributes are to be projected
		List<Attribute> attrsToProjectFromOp = new ArrayList<>(attrs);
		attrsToProjectFromOp.retainAll(op.getOutput().getAttributes());
		
		// not all attributes from op is necessary
		if (attrsToProjectFromOp.size() > 0) {
			Operator op2 = new Project(op, attrsToProjectFromOp);
			op2.accept(est);
			return op2;
		} else {
			return op;
		}
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

	public static Operator buildProductOrJoin(List<Operator> ops, List<Predicate> preds){
		
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
			//ops.add(newResult);
			Set<Attribute> neededAttrs = getNecessaryAttrs(preds, top);
			if (neededAttrs.size() == newResult.getOutput().getAttributes().size() &&
					newResult.getOutput().getAttributes().containsAll(neededAttrs)){
				ops.add(newResult);
			}else{
				List<Attribute> neededFromNowOn = newResult.getOutput().getAttributes().stream().filter(attr -> neededAttrs.contains(attr)).collect(Collectors.toList());
				if (neededFromNowOn.size() == 0)
					ops.add(newResult);
				else {
					Project tempProj = new Project(newResult, neededFromNowOn);
					tempProj.accept(est);
					ops.add(tempProj);
				}
			}
		}
		
		
		
		result = ops.get(0);
		
		return ops.get(0);
	}
	
	private static Set<Attribute> getNecessaryAttrs(List<Predicate> predicates, Operator top){
		Set<Attribute> attrsNeeded = new HashSet<>();
		Iterator<Predicate> predIt = predicates.iterator();
		while(predIt.hasNext()){
			Predicate currentPred = predIt.next();
			Attribute left = currentPred.getLeftAttribute();
			Attribute right = currentPred.getRightAttribute();
			attrsNeeded.add(left);
			if (right != null) attrsNeeded.add(right);
		}
		if (top instanceof Project) attrsNeeded.addAll(((Project) top).getAttributes());
		return attrsNeeded;
	}
	
	private List<List<Predicate>> generatePerm(List<Predicate> original) {
		if (original.size() == 0) { 
			List<List<Predicate>> result = new ArrayList<List<Predicate>>();
		    result.add(new ArrayList<Predicate>());
		    return result;
		}
		Predicate firstElement = original.remove(0);
		List<List<Predicate>> returnValue = new ArrayList<List<Predicate>>();
		List<List<Predicate>> permutations = generatePerm(original);
		for (List<Predicate> smallerPermutated : permutations) {
		    for (int index=0; index <= smallerPermutated.size(); index++) {
		    	List<Predicate> temp = new ArrayList<Predicate>(smallerPermutated);
		    	temp.add(index, firstElement);
		    	returnValue.add(temp);
		    }
		}
		return returnValue;
	}

	class Sections implements PlanVisitor {
		
		private Set<Attribute> allAttributes = new HashSet<>();
		private Set<Predicate> allPredicates = new HashSet<>();
		private Set<Scan> allScans = new HashSet<Scan>();
		

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
