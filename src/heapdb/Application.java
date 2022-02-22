package heapdb;

import query.*;

public class Application {
    public static void main(String[] args) {

        // ADD INSTRUCTOR
        Schema schema_inst = new Schema();
        schema_inst.addKeyIntType("ID");   // primary key ID int
        schema_inst.addVarCharType("name");
        schema_inst.addVarCharType("dept_name");
        schema_inst.addIntType("salary");
        Tuple[] tuples_inst = new Tuple[]{
                new Tuple(schema_inst, 10101, "Srinivasan", "Comp. Sci.", 65000),
                new Tuple(schema_inst, 12121, "Wu", "Finance", 90000),
                new Tuple(schema_inst, 15151, "Mozart", "Music", 40000),
                new Tuple(schema_inst, 22222, "Einstein", "Physics", 95000),
                new Tuple(schema_inst, 32343, "El Said", "History", 60000),
                new Tuple(schema_inst, 33456, "Gold", "Physics", 87000),
                new Tuple(schema_inst, 45565, "Katz", "Comp. Sci.", 75000),
                new Tuple(schema_inst, 58583, "Califieri", "History", 62000),
                new Tuple(schema_inst, 76543, "Singh", "Finance", 80000),
                new Tuple(schema_inst, 76766, "Crick", "Biology", 72000),
                new Tuple(schema_inst, 83821, "Brandt", "Comp. Sci.", 92000),
                new Tuple(schema_inst, 98345, "Kim", "Elec. Eng.", 80000)
        };
        Table table_inst = new Table(schema_inst);
        for (Tuple tuple : tuples_inst) {
            table_inst.insert(tuple);
        }
        System.out.println(table_inst);

        // ADD DEPARTMENT
        Schema schema_dept = new Schema();
        schema_dept.addKeyVarCharType("dept_name");
        schema_dept.addVarCharType("building");
        schema_dept.addIntType("budget");
        Tuple[] tuples_dept = new Tuple[]{
                new Tuple(schema_dept, "Biology", "Watson", 90000),
                new Tuple(schema_dept, "Comp. Sci.", "Taylor", 100000),
                new Tuple(schema_dept, "Elec. Eng.", "Taylor", 85000),
                new Tuple(schema_dept, "Finance", "Painter", 120000),
                new Tuple(schema_dept, "History", "Painter", 50000),
                new Tuple(schema_dept, "Music", "Packard", 80000),
                new Tuple(schema_dept, "Physics", "Watson", 70000),

        };
        Table table_dept = new Table(schema_dept);
        for (Tuple tuple: tuples_dept) {
            table_dept.insert(tuple);
        }
        System.out.println(table_dept);


//        // delete
//        table.delete(12121);
//        System.out.println(table);
//
//        // lookup ok
//        System.out.print("lookup 19803: ");
//        table.lookup(19803);
//
//        // lookup but not here
//        System.out.print("lookup 12121: ");
//        table.lookup(12121);
//
//        // lookup from department name
//        System.out.println("lookup dept_name=Comp. Sci.: ");
//        table.lookup("dept_name", "Comp. Sci.");
//
//        // lookup
//        System.out.println("lookup ID=19803: ");
//        table.lookup("ID", 19803);
//
//        // lookup
//        System.out.println("lookup ID=19802: ");
//        table.lookup("ID", 19802);
//
//        // eval1
//        Condition cond = new EqCondition("dept_name", "Comp. Sci.");
//        SelectQuery q = new SelectQuery(cond);
//        ITable result = q.eval(table);
//        System.out.println(result);
//
//        // eval2
//        cond = new AndCondition(new EqCondition("salary", 70000), new EqCondition("name", "Bruns"));
//        q = new SelectQuery(cond);
//        result = q.eval(table);
//        System.out.println(result);
//
//        // eval3
//        cond = new OrCondition(new AndCondition(new EqCondition("salary", 70000), new EqCondition("name", "Bruns")), new AndCondition(new EqCondition("name", "Scott"), new EqCondition("dept_name", "Math")));
//        q = new SelectQuery(cond);
//        result = q.eval(table);
//        System.out.println(result);
    }
}
