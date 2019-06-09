package edu.ecnu.touchstone.pretreatment;

import edu.ecnu.touchstone.constraintchain.*;
import edu.ecnu.touchstone.queryinstantiation.ComputingThreadPool;
import edu.ecnu.touchstone.queryinstantiation.Parameter;
import edu.ecnu.touchstone.queryinstantiation.QueryInstantiator;
import edu.ecnu.touchstone.run.Touchstone;
import edu.ecnu.touchstone.schema.Attribute;
import edu.ecnu.touchstone.schema.ForeignKey;
import edu.ecnu.touchstone.schema.SchemaReader;
import edu.ecnu.touchstone.schema.Table;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;
import java.util.Map.Entry;

// main functions: 
// 1. get the partial order among tables
// 2. get the generation templates of tables
public class Preprocessor {

    private List<Table> tables;
    private List<ConstraintChain> constraintChains;
    private List<Parameter> parameters;
    private Logger logger;

    public Preprocessor(List<Table> tables, List<ConstraintChain> constraintChains, List<Parameter> parameters) {
        super();
        this.tables = tables;
        this.constraintChains = null;
        this.constraintChains = constraintChains;
        this.parameters = parameters;
        logger = Logger.getLogger(Touchstone.class);
    }

    // get the partial order among tables according to the foreign key constraints
    public List<String> getPartialOrder() {
        Set<String> allTables = new HashSet<>();
        // the tables with a foreign key (or foreign keys)
        Set<String> nonMetaTables = new HashSet<>();
        // map: table -> referenced tables (foreign key constraint)
        Map<String, ArrayList<String>> tableDependencyInfo = new HashMap<>();
        for (Table table : tables) {
            allTables.add(table.getTableName());
            if (table.getForeignKeys().size() != 0) {
                nonMetaTables.add(table.getTableName());
                List<ForeignKey> foreignKeys = table.getForeignKeys();
                ArrayList<String> referencedTables = new ArrayList<>();
                for (ForeignKey foreignKey : foreignKeys) {
                    referencedTables.add(foreignKey.getReferencedKey().split("\\.")[0]);
                }
                tableDependencyInfo.put(table.getTableName(), referencedTables);
            }
        }

        // the remaining tables are metadata tables
        allTables.removeAll(nonMetaTables);
        Set<String> partialOrder = new LinkedHashSet<>(allTables);
        Iterator<Entry<String, ArrayList<String>>> iterator = tableDependencyInfo.entrySet().iterator();
        while (true) {
            while (iterator.hasNext()) {
                Entry<String, ArrayList<String>> entry = iterator.next();
                if (partialOrder.containsAll(entry.getValue())) {
                    partialOrder.add(entry.getKey());
                }
            }
            if (partialOrder.size() == tables.size()) {
                break;
            }
            iterator = tableDependencyInfo.entrySet().iterator();
        }

        logger.debug("\nThe partial order of tables: \n\t" + partialOrder);
        return new ArrayList<>(partialOrder);
    }

    // get the generation templates of all tables
    public Map<String, TableGeneTemplate> getTableGeneTemplates(int shuffleMaxNum, int pkvsMaxSize) {
        Map<Integer, Parameter> parameterMap = new HashMap<>();
        for (Parameter parameter : parameters) {
            parameterMap.put(parameter.getId(), parameter);
        }

        Map<String, TableGeneTemplate> tableGeneTemplateMap = new HashMap<>();
        for (Table table : tables) {
            String tableName = table.getTableName();
            long tableSize = table.getTableSize();
            String pkStr = table.getPrimaryKey().toString();
            List<Key> keys = new ArrayList<>();
            List<Attribute> attributes = table.getAttributes();
            List<ConstraintChain> tableConstraintChains = new ArrayList<>();
            List<String> referencedKeys = new ArrayList<>();
            Map<String, String> referKeyForeKeyMap = new HashMap<>();
            Map<Integer, Parameter> localParameterMap = new HashMap<>();
            Map<String, Attribute> attributeMap = new HashMap<>();

            // keys
            List<String> primaryKey = table.getPrimaryKey();
            List<ForeignKey> foreignKeys = table.getForeignKeys();
            loop:
            for (String s : primaryKey) {
                for (ForeignKey foreignKey : foreignKeys) {
                    if (foreignKey.getAttrName().equals(s.split("\\.")[1])) {
                        continue loop;
                    }
                }
                keys.add(new Key(s, 0));
            }
            // add an attribute to ensure the uniqueness of the primary key
            if (keys.size() == 0) {
                keys.add(new Key("unique_number", 0));
            }
            for (ForeignKey foreignKey : foreignKeys) {
                keys.add(new Key(tableName + "." + foreignKey.getAttrName(), 1));
            }

            // tableConstraintChains
            for (ConstraintChain constraintChain : constraintChains) {
                if (constraintChain.getTableName().equals(tableName)) {
                    tableConstraintChains.add(constraintChain);
                }
            }

            //nullProbability
            Map<Integer, Double> keyNullProbability = new HashMap<>();
            for (ConstraintChain tableConstraintChain : tableConstraintChains) {
                for (CCNode node : tableConstraintChain.getNodes()) {
                    if (node.getType() == 1) {
                        PKJoin pkJoin = (PKJoin) node.getNode();
                        double[] nullProbability = pkJoin.getNullProbability();
                        for (int i = 0; i < nullProbability.length; i++) {
                            if (nullProbability[i] > 0) {
                                keyNullProbability.put(pkJoin.getCanJoinNum()[i], pkJoin.getNullProbability()[i]);
                            }
                        }
                    }
                }
            }


            // referencedKeys (support mixed reference)
            foreignKeys.sort(Comparator.comparing(ForeignKey::getReferencedKey));
            for (int index = 0, j = 0; j < foreignKeys.size(); j++) {
                if ((j < foreignKeys.size() - 1)) {
                    if (foreignKeys.get(j).getReferencedKey().split("\\.")[0].equals(
                            foreignKeys.get(j + 1).getReferencedKey().split("\\.")[0])) {
                        continue;
                    }
                }
                StringBuilder fksStr = new StringBuilder("[");
                for (int k = index; k <= j; k++) {
                    fksStr.append(foreignKeys.get(k).getReferencedKey());
                    if (k != j) {
                        fksStr.append(", ");
                    }
                }
                fksStr.append("]");
                referencedKeys.add(fksStr.toString());
                index = j + 1;
            }

            // referKeyForeKeyMap
            for (ForeignKey foreignKey : foreignKeys) {
                referKeyForeKeyMap.put(foreignKey.getReferencedKey(),
                        tableName + "." + foreignKey.getAttrName());
            }

            // localParameterMap
            for (ConstraintChain tableConstraintChain : tableConstraintChains) {
                List<CCNode> nodes = tableConstraintChain.getNodes();
                for (CCNode node : nodes) {
                    if (node.getType() == 0) {
                        Filter filter = (Filter) node.getNode();
                        FilterOperation[] operations = filter.getFilterOperations();
                        for (FilterOperation operation : operations) {
                            localParameterMap.put(operation.getId(), parameterMap.get(operation.getId()));
                        }
                    }
                }
            }

            // attributeMap
            for (Attribute attribute : attributes) {
                attributeMap.put(attribute.getAttrName(), attribute);
            }

            TableGeneTemplate tableGeneTemplate = new TableGeneTemplate(tableName, tableSize, pkStr,
                    keys, attributes, tableConstraintChains, referencedKeys, referKeyForeKeyMap,
                    localParameterMap, attributeMap, shuffleMaxNum, pkvsMaxSize, keyNullProbability);
            tableGeneTemplateMap.put(tableName, tableGeneTemplate);
        }

        logger.debug("\nThe generation template map of tables: \n" + tableGeneTemplateMap);
        return tableGeneTemplateMap;
    }

    // test
    public static void main(String[] args) throws Exception {
        PropertyConfigurator.configure(".//test//lib//log4j.properties");
        System.setProperty("com.wolfram.jlink.libdir",
                "C://Program Files//Wolfram Research//Mathematica//10.0//SystemFiles//Links//JLink");

        SchemaReader schemaReader = new SchemaReader();
        List<Table> tables = schemaReader.read(".//test//input//tpch_schema_sf_1.txt");
        ConstraintChainsReader constraintChainsReader = new ConstraintChainsReader();
        List<ConstraintChain> constraintChains = constraintChainsReader.read(".//test//input//tpch_cardinality_constraints_sf_1.txt");
        ComputingThreadPool computingThreadPool = new ComputingThreadPool(4, 20, 0.00001);
        QueryInstantiator queryInstantiator = new QueryInstantiator(tables, constraintChains, null, 20, 0.00001, computingThreadPool);
        queryInstantiator.iterate();
        List<Parameter> parameters = queryInstantiator.getParameters();

        Preprocessor preprocessor = new Preprocessor(tables, constraintChains, parameters);
        preprocessor.getPartialOrder();
        Map<String, TableGeneTemplate> tableGeneTemplateMap = preprocessor.getTableGeneTemplates(1000, 10000);

        TableGeneTemplate template = tableGeneTemplateMap.entrySet().iterator().next().getValue();
        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(".//data//template"));
        oos.writeObject(template);
        oos.close();
        ObjectInputStream ois = new ObjectInputStream(new FileInputStream(".//data//template"));
        TableGeneTemplate template2 = (TableGeneTemplate) ois.readObject();
        ois.close();
        System.out.println("-----------------------");
        System.out.println(template);
        System.out.println(template2);
    }
}
