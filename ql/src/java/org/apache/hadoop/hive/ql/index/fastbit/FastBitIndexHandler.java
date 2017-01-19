package org.apache.hadoop.hive.ql.index.fastbit;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.index.HiveIndexQueryContext;
import org.apache.hadoop.hive.ql.index.TableBasedIndexHandler;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.IndexUtils;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

public class FastBitIndexHandler extends TableBasedIndexHandler {
    @Override
    public void analyzeIndexDefinition(Table baseTable, Index index, Table indexTable) throws HiveException {
        StorageDescriptor storageDesc = index.getSd();
        if (this.usesIndexTable() && indexTable != null) {
            StorageDescriptor indexTableSd = storageDesc.deepCopy();
            List<FieldSchema> indexTblCols = indexTableSd.getCols();
            FieldSchema bucketFileName = new FieldSchema("_bucketname", "string", "");
            indexTblCols.add(bucketFileName);
            FieldSchema offSets = new FieldSchema("_offset", "bigint", "");
            indexTblCols.add(offSets);
            FieldSchema bitmaps = new FieldSchema("_bitmaps", "array<bigint>", "");
            indexTblCols.add(bitmaps);
            indexTable.setSd(indexTableSd);
        }
    }

    @Override
    protected Task<?> getIndexBuilderMapRedTask(Set<ReadEntity> inputs, Set<WriteEntity> outputs,
                                                List<FieldSchema> indexField, boolean partitioned,
                                                PartitionDesc indexTblPartDesc, String indexTableName,
                                                PartitionDesc baseTablePartDesc, String baseTableName, String dbName) throws HiveException {

        HiveConf builderConf = new HiveConf(getConf(), FastBitIndexHandler.class);
        HiveConf.setBoolVar(builderConf, HiveConf.ConfVars.HIVEROWOFFSET, true);

        String indexCols = HiveUtils.getUnparsedColumnNamesFromFieldSchema(indexField);

        //form a new insert overwrite query.
        StringBuilder command= new StringBuilder();
        LinkedHashMap<String, String> partSpec = indexTblPartDesc.getPartSpec();

        command.append("INSERT OVERWRITE TABLE " +
                HiveUtils.unparseIdentifier(dbName) + "." + HiveUtils.unparseIdentifier(indexTableName ));
        if (partitioned && indexTblPartDesc != null) {
            command.append(" PARTITION ( ");
            List<String> ret = getPartKVPairStringArray(partSpec);
            for (int i = 0; i < ret.size(); i++) {
                String partKV = ret.get(i);
                command.append(partKV);
                if (i < ret.size() - 1) {
                    command.append(",");
                }
            }
            command.append(" ) ");
        }

        command.append(" SELECT ");
        command.append(indexCols);
        command.append(",");
        command.append(VirtualColumn.FILENAME.getName());
        command.append(",");
        command.append(VirtualColumn.BLOCKOFFSET.getName());
        command.append(",");
        command.append("FASTBIT(");
        command.append(VirtualColumn.ROWOFFSET.getName());
        command.append(")");
        command.append(" FROM " +
                HiveUtils.unparseIdentifier(dbName) + "." + HiveUtils.unparseIdentifier(baseTableName));
        LinkedHashMap<String, String> basePartSpec = baseTablePartDesc.getPartSpec();
        if(basePartSpec != null) {
            command.append(" WHERE ");
            List<String> pkv = getPartKVPairStringArray(basePartSpec);
            for (int i = 0; i < pkv.size(); i++) {
                String partKV = pkv.get(i);
                command.append(partKV);
                if (i < pkv.size() - 1) {
                    command.append(" AND ");
                }
            }
        }
        command.append(" GROUP BY ");
        command.append(VirtualColumn.FILENAME.getName());
        command.append(",");
        command.append(VirtualColumn.BLOCKOFFSET.getName());
        for (FieldSchema fieldSchema : indexField) {
            command.append(",");
            command.append(HiveUtils.unparseIdentifier(fieldSchema.getName()));
        }

        // Require clusterby ROWOFFSET if map-size aggregation is off.
        // TODO: Make this work without map side aggregation
        if (!builderConf.get("hive.map.aggr", null).equals("true")) {
            throw new HiveException("Cannot construct index without map-side aggregation");
        }

        Task<?> rootTask = IndexUtils.createRootTask(builderConf, inputs, outputs,
                command, partSpec, indexTableName, dbName);
        return rootTask;
    }



    @Override
    public void generateIndexQuery(List<Index> indexes, ExprNodeDesc predicate, ParseContext pctx, HiveIndexQueryContext queryContext) {

    }

    @Override
    public boolean usesIndexTable() {
        return true;
    }
}