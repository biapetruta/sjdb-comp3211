package test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import sjdb.Catalogue;
import sjdb.DatabaseException;
import sjdb.Estimator;
import sjdb.Predicate;
import sjdb.Scan;

public class OptimiserTest {
	
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
		
		System.out.println("========  printing catalogue  ========\n");
		try {
			System.out.println(cat.getRelation("A").render());
			System.out.println(cat.getRelation("B").render());
			System.out.println(cat.getRelation("C").render());
		} catch (DatabaseException e) {
			fail("Catalogue NOT created Properly");
			System.exit(-1);
		}
	}

	@Test
	public void testBuildSelectsOnTop() {
		Scan scan_a = null;
		Scan scan_b = null;
 		Scan scan_c = null;
		try {
			scan_a = new Scan(cat.getRelation("A"));
			System.out.println("S1 ===> " + scan_a.getRelation().render());
		} catch (DatabaseException e) { fail("Relation A NOT in catalogue!"); }
		try {
			scan_b = new Scan(cat.getRelation("B"));
			System.out.println("S2 ===> " + scan_b.getRelation().render());
		} catch (DatabaseException e) { fail("Relation B NOT in catalogue!"); }
		try {
			scan_c = new Scan(cat.getRelation("C"));
			System.out.println("S3 ===> " + scan_c.getRelation().render());
		} catch (DatabaseException e) { fail("Relation C NOT in catalogue!"); }
		List<Predicate> preds = new ArrayList<>();
		fail("FUCK U");
	}

}
