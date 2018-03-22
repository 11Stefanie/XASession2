package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.Distinct;
import com.xingcloud.xa.session2.ra.Group;
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
		Map<String, List<Object[]>> groupRows = getGroupRows();

		//4. column filter
		columnFilter(groupRows);

		//5. projection of each group
		//TODO
		Map<String, List<Object[]>> groupResults = getGroupResults(groupRows);

		//6. combine the results of each group
		//TODO
		return null;
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

	private Map<String, List<Object[]>> getGroupRows() {
		Map<String, List<Object[]>> groups = new HashMap<>();
		RowIterator inputIterator = relation.iterator();
		while (inputIterator.hasNext()) {
			XRelation.XRow inputRow = (XRelation.XRow) inputIterator.nextRow();
			String groupKeyStr = getGroupKey(inputRow);
			List<Object[]> groupRows = groups.get(groupKeyStr);
			if (groupRows == null) {
				groupRows = new ArrayList<>();
			}
			groupRows.add(inputRow.rowData);
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

	private void columnFilter(Map<String, List<Object[]>> groupRows) {
		for (List<Object[]> groupRowList : groupRows.values()) {

		}
		//TODO
	}

	private Map<String, List<Object[]>> getGroupResults(Map<String, List<Object[]>> groupRows) {
		Map<String, List<Object[]>> groupResults = new HashMap<>();
		for (List<Object[]> groupRow : groupRows.values()) {
			//TODO
		}
		return groupResults;
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
