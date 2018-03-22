package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.Distinct;
import com.xingcloud.xa.session2.ra.Group;
import com.xingcloud.xa.session2.ra.Projection;
import com.xingcloud.xa.session2.ra.Relation;
import com.xingcloud.xa.session2.ra.RelationProvider;
import com.xingcloud.xa.session2.ra.RowIterator;
import com.xingcloud.xa.session2.ra.expr.ColumnValue;
import com.xingcloud.xa.session2.ra.expr.Constant;
import com.xingcloud.xa.session2.ra.expr.Expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author: mulisen
 * Date:   2/7/13
 */
public class XGroup extends AbstractOperation implements Group {


	RelationProvider relation;

	Expression[] groupingExpressions;

	Expression[] projectionExpressions;

	public Relation evaluate() {
		//1. get input
		RowIterator inputterator = relation.iterator();

		//2. get columnMap &  expressions
		Map<String, Integer> originColumnMap = getOriginColumnMap();
		Map<String, Integer> columnMap = new HashMap<>();
		List<Expression> expressionList = new ArrayList<>();
		int columnIndex = 0;
		for (Expression expression : projectionExpressions) {
			expressionList.add(expression);
			columnMap.put(getColumnName(expression), columnIndex++);
		}

		//3. group inputRows
		Map<String, List<XRelation.XRow>> groupRows = getGroupRows();

		//4. column filter
		Map<String, Projection> groupProjections = getGroupProjections(columnMap, groupRows);

		//5. projection of each group
		Map<String, Relation> groupResults = getGroupResults(groupProjections);

		//6. combine the results of each group
		XRelation relation = getGroupRelation(columnMap, groupResults);
		return relation;
	}

	private Map<String, Integer> getOriginColumnMap() {
		Map<String, Integer> columnMap = new HashMap<>();
		int columnIndex = 0;
		for (Expression expression : groupingExpressions) {
			if (expression instanceof ColumnValue) {
				columnMap.put(((ColumnValue) expression).columnName, columnIndex++);
			}
		}
		return columnMap;
	}

	private String getColumnName(Expression expression) {
		if (expression instanceof ColumnValue) {
			return ((ColumnValue) expression).columnName;
		}
		if (expression instanceof Constant) {
			return String.valueOf(((Constant) expression).value);
		}
		if (expression instanceof Distinct) {

		}

		//TODO
		return "c";
	}

	private Map<String, List<XRelation.XRow>> getGroupRows() {
		Map<String, List<XRelation.XRow>> groups = new HashMap<>();
		RowIterator inputIterator = relation.iterator();
		while (inputIterator.hasNext()) {
			XRelation.XRow inputRow = (XRelation.XRow) inputIterator.nextRow();
			String groupKeyStr = getGroupKey(inputRow);
			List<XRelation.XRow> groupRows = groups.get(groupKeyStr);
			if (groupRows == null) {
				groupRows = new ArrayList<>();
			}
			groupRows.add(inputRow);
			groups.put(groupKeyStr, groupRows);
		}
		return groups;
	}

	private String getGroupKey(XRelation.XRow inputRow) {
		StringBuilder groupKeySb = new StringBuilder();
		Map<String, Integer> columnMap = inputRow.columnNames;
		for (Expression expression : groupingExpressions) {
			String columnName = ((ColumnValue) expression).columnName;
			groupKeySb.append(inputRow.rowData[columnMap.get(columnName)]);
		}
		return groupKeySb.toString();
	}

	private Map<String, Projection> getGroupProjections(Map<String, Integer> columnIndexMap,
																											Map<String, List<XRelation.XRow>> groupRows) {
		Map<String, Projection> groupProjections = new HashMap<>();
		for (String key : groupRows.keySet()) {
			List<XRelation.XRow> groupRowList = groupRows.get(key);
			List<Object[]> rowSet = new ArrayList<>();
			int rowLength = projectionExpressions.length;
			for (XRelation.XRow groupRow : groupRowList) {
				Object[] row = new Object[rowLength];
				int columnIndex = 0;
				for (Expression expression : projectionExpressions) {
					row[columnIndex++] = expression.evaluate(groupRow);
				}
				rowSet.add(row);
			}
			Projection projection = genProjection(columnIndexMap, rowSet);
			groupProjections.put(key, projection);
		}
		return groupProjections;
	}

	private Projection genProjection(Map<String, Integer> columnIndexMap, List<Object[]> rowSet) {
		Projection projection = new XProjection();
		Relation relation = new XRelation(columnIndexMap, rowSet);
		//TODO
		projection.setInput(relation, projectionExpressions);
		return projection;
	}

	private Map<String, Relation> getGroupResults(Map<String, Projection> projectionMap) {
		Map<String, Relation> groupResults = new HashMap<>();
		for (String key : projectionMap.keySet()) {
			Projection projection = projectionMap.get(key);
			Relation relation = projection.evaluate();
			groupResults.put(key, relation);
		}
		return groupResults;
	}

	private XRelation getGroupRelation(Map<String, Integer> columnMap, Map<String, Relation> groupRelationMap) {
		List<Object[]> rowSet = new ArrayList<>();
		for (Relation relation : groupRelationMap.values()) {
			RowIterator rowIterator = relation.iterator();

			while (rowIterator.hasNext()) {
				XRelation.XRow xRow = (XRelation.XRow) rowIterator.nextRow();
				rowSet.add(xRow.rowData);
			}
		}

		return new XRelation(columnMap, rowSet);
	}

	public Group setInput(RelationProvider relation, Expression[] groupingExpressions,
												Expression[] projectionExpressions) {
		resetInput();
		this.relation = relation;
		this.groupingExpressions = groupingExpressions;
		this.projectionExpressions = projectionExpressions;
		addInput(relation);
		return this;
	}

	@Override
	public String toString() {
		return IndentPrint.print(this);
	}

}
