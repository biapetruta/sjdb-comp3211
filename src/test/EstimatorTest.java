package test;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import org.junit.BeforeClass;
import org.junit.Test;

import sjdb.Attribute;
import sjdb.Catalogue;
import sjdb.DatabaseException;
import sjdb.Estimator;
import sjdb.Operator;
import sjdb.Predicate;
import sjdb.Product;
import sjdb.Project;
import sjdb.Scan;
import sjdb.Select;

public class EstimatorTest {
	
	private static Catalogue cat;
	private static Estimator est;
	
	@BeforeClass
	public static void createCatalouge(){
		est = new Estimator();
		cat = new Catalogue();
		
		System.out.println("========  creating catalogue  ========");
		
		System.out.println("=== creating relations ===");
		
		cat.createRelation("A", 1000);
		cat.createRelation("B", 2000);
		cat.createRelation("C", 3000);
		
		System.out.println("=== creating attributes ===");
		
		cat.createAttribute("A", "a", 100);
		cat.createAttribute("A", "b", 200);
		cat.createAttribute("A", "c", 300);
		cat.createAttribute("B", "d", 300);
		cat.createAttribute("B", "e", 200);
		cat.createAttribute("B", "f", 100);
		cat.createAttribute("C", "g", 400);
		cat.createAttribute("C", "h", 500);
		cat.createAttribute("C", "i", 600);
		
		System.out.println("========  printing catalogue  ========");
		try {
			System.out.println(cat.getRelation("A").render());
			System.out.println(cat.getRelation("B").render());
			System.out.println(cat.getRelation("C").render());
		} catch (DatabaseException e) {
			fail("Catalogue NOT created Properly");
			System.exit(-1);
		}
	}
	
	// test for scan(R)
	@Test
	public void testVisitScan() {
		try {
			Operator scan_a = new Scan(cat.getRelation("A"));
			scan_a.accept(est);
			assertEquals(1000, scan_a.getOutput().getTupleCount());
		} catch (DatabaseException e) { fail("Relation A NOT in catalogue!"); }
	}

	// test for project single attr
	@Test
	public void testVisitProjectSingleAttr() {
		try {
			Attribute attrs[] = {cat.getAttribute("a")};
			Operator input = new Scan(cat.getRelation("A"));
			Project project_Aa = new Project(input, Arrays.asList(attrs));
			project_Aa.accept(est);
			est(input, project_Aa);
			checkAttr(project_Aa);
		} catch (DatabaseException e) { fail("Relation A || Attribute a NOT in catalogue!"); }
	}
	
	// test for project multiple attr
	@Test
	public void testVisitProjectMultipleAttr() {
		try {
			Attribute attrs[] = {cat.getAttribute("a"), cat.getAttribute("b"), cat.getAttribute("c")};
			Operator input = new Scan(cat.getRelation("A"));
			Project project_Aabc = new Project(input, Arrays.asList(attrs));
			project_Aabc.accept(est);
			est(input, project_Aabc);
			checkAttr(project_Aabc);
		} catch (DatabaseException e) { fail("Relation A || Attribute a NOT in catalogue!"); }
	}

	// test for select attr = val
	@Test
	public void testVisitSelectVal() {
		try {
			Predicate pred = new Predicate(cat.getAttribute("a"), "a_1");
			Operator input = new Scan(cat.getRelation("A"));
			Select select_A_a_val = new Select(input, pred);
			select_A_a_val.accept(est);
			est(input, select_A_a_val);
			checkAttr(select_A_a_val);
		} catch (DatabaseException e) { fail("Relation A || Attribute a NOT in catalogue!"); }
	}
	
	// test for select attr = attr
	@Test
	public void testVisitSelectAttr() {
		try {
			Predicate pred = new Predicate(cat.getAttribute("a"), cat.getAttribute("b"));
			Operator input = new Scan(cat.getRelation("A"));
			Select select_A_a_val = new Select(input, pred);
			select_A_a_val.accept(est);
			est(input, select_A_a_val);
		} catch (DatabaseException e) { fail("Relation A || Attribute a || Attribute b NOT in catalogue!"); }
	}

	// test for product
	@Test
	public void testVisitProduct() {
		try {
			Operator left = new Scan(cat.getRelation("A"));
			Operator right = new Scan(cat.getRelation("B"));
			Product product_ab = new Product(left, right);
			product_ab.accept(est);
			est(left, right, product_ab);
			checkAttr(product_ab);
		} catch (DatabaseException e) { fail("Relation A || B NOT in catalogue!"); }
	}

	@Test
	public void testVisitJoin() {
		fail("Not yet implemented");
	}
	
	// check the estimated output relation size for PROJECT
	private static void est(Operator input, Project project_Aa){
		// T(PROJECT(R)) = T(R)
		assertEquals(input.getOutput().getTupleCount(), project_Aa.getOutput().getTupleCount());
	}
	
	// check the estimated output relation size for SELECT
	private static void est(Operator input, Select select_A_a_val){
		Predicate pred = select_A_a_val.getPredicate();
		if (select_A_a_val.getPredicate().equalsValue())
			// T(SELECT_a=val(R)) = T(R) / V(R, a)
			assertEquals(input.getOutput().getTupleCount()/pred.getLeftAttribute().getValueCount(), select_A_a_val.getOutput().getTupleCount());
		else
			// T(SELECT_a=b(R)) = T(R) / max(V(R, a), V(R, b))
			assertEquals(input.getOutput().getTupleCount()/(Math.max(pred.getLeftAttribute().getValueCount(), pred.getRightAttribute().getValueCount())), select_A_a_val.getOutput().getTupleCount());
	}
	
	// check the estimated output relation size for PRODUCT
	private static void est(Operator left, Operator right, Product product_ab) {
		// T(PRDUCT_RS) = T(R) x T(S)
		assertEquals(left.getOutput().getTupleCount() * right.getOutput().getTupleCount(), product_ab.getOutput().getTupleCount());
	}

	// check the attributes and valueCounts for PROJECT
	private static void checkAttr(Project project_Aa){
		HashMap<String, Integer> inp_attrs = new HashMap<>(project_Aa.getAttributes().size());
		HashMap<String, Integer> out_attrs = new HashMap<>(project_Aa.getOutput().getAttributes().size());
		
		project_Aa.getAttributes().forEach(attr -> inp_attrs.put(attr.getName(), attr.getValueCount()));
		project_Aa.getOutput().getAttributes().forEach(attr -> out_attrs.put(attr.getName(), attr.getValueCount()));
		
		// only those that were meant to be projected are projected with the SAME valueCount
		assertEquals(inp_attrs.size(), out_attrs.size());
		inp_attrs.keySet().forEach(attr -> assertEquals(inp_attrs.get(attr), out_attrs.get(attr)));
	}
	
	// check the attributes and valueCounts for SELECT
	private static void checkAttr(Select select_Aa){
		// check if all and only the input attributes were added
		assertTrue(select_Aa.getInput().getOutput().getAttributes().size() == select_Aa.getOutput().getAttributes().size() &&
				   select_Aa.getOutput().getAttributes().containsAll(select_Aa.getInput().getOutput().getAttributes()));
		if (select_Aa.getPredicate().equalsValue()) checkAttrVal(select_Aa);
		else checkAttrAttr(select_Aa);
	}
	
	private static void checkAttrVal(Select select_A_a_val){
		Attribute pred_attr = select_A_a_val.getPredicate().getLeftAttribute();
		// if attr = val, V(R, a) in output = 1
		assertEquals(((Attribute) (filterOutputAttributes(select_A_a_val, attr -> attr.equals(pred_attr))[0])).getValueCount(), 1);
	}
	
	private static void checkAttrAttr(Select select_Aab){
		Attribute left_attr = select_Aab.getPredicate().getLeftAttribute();
		Attribute right_attr = select_Aab.getPredicate().getRightAttribute();
		
		int left_count = ((Attribute) (filterOutputAttributes(select_Aab, attr -> attr.equals(left_attr))[0])).getValueCount();
		int right_count = ((Attribute) (filterOutputAttributes(select_Aab, attr -> attr.equals(right_attr))[0])).getValueCount();
		int actual = Math.min(left_attr.getValueCount(), right_attr.getValueCount());
		
		// if attr = attr, V(Output, a) = V(Output, b) = min(V(R, a), V(R, b))
		assertEquals(left_count, actual);
		assertEquals(right_count, actual);
	}
	
	private static Object[] filterOutputAttributes(Operator op, java.util.function.Predicate<? super Attribute> pred) {
		return op.getOutput().getAttributes()
				.stream()
				.filter(pred)
				.toArray();
	}
	
	// check the attributes and valueCounts for PRODUCT
	private static void checkAttr(Product product_ab){
		HashMap<String, Integer> inp_attrs = new HashMap<>(product_ab.getLeft().getOutput().getAttributes().size() + product_ab.getRight().getOutput().getAttributes().size());
		HashMap<String, Integer> out_attrs = new HashMap<>(product_ab.getOutput().getAttributes().size());
		
		product_ab.getLeft().getOutput().getAttributes().forEach(attr -> inp_attrs.put(attr.getName(), attr.getValueCount()));
		product_ab.getRight().getOutput().getAttributes().forEach(attr -> inp_attrs.put(attr.getName(), attr.getValueCount()));
		product_ab.getOutput().getAttributes().forEach(attr -> out_attrs.put(attr.getName(), attr.getValueCount()));;
		
		// check to see if all and only the attributes from left and right were added with SAME valueCount.
		assertEquals(inp_attrs.size(), out_attrs.size());
		inp_attrs.keySet().forEach(attr -> assertEquals(inp_attrs.get(attr), out_attrs.get(attr)));
	}
}
