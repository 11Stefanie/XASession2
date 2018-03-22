package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.Distinct;
import com.xingcloud.xa.session2.ra.Relation;
import com.xingcloud.xa.session2.ra.RelationProvider;
import com.xingcloud.xa.session2.ra.Row;
import com.xingcloud.xa.session2.ra.RowIterator;
import com.xingcloud.xa.session2.ra.expr.ColumnValue;
import com.xingcloud.xa.session2.ra.expr.Expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author: mulisen
 * Date:   2/6/13
 */
public class XDistinct extends AbstractOperation implements Distinct {

	RelationProvider relation;
	Expression[] expressions;

	public Relation evaluate() {
		//1. get columnMap
		Map<String, Integer> columnMap = getColumnMap();

		//2. get input
		RowIterator inputIterator = relation.iterator();

		//3. get rows
		List<Object[]> rowSet = getRowSet(relation.iterator());
		return new XRelation(columnMap, rowSet);  //TODO method implementation
	}

	private Map<String, Integer> getColumnMap() {
		Map<String, Integer> columnMap = new HashMap<>();
		int columnIndex = 0;
		for (Expression expression : expressions) {
			if (expression instanceof ColumnValue) {
				columnMap.put(((ColumnValue) expression).columnName, columnIndex++);
			}
		}
		return columnMap;
	}

	private List<Object[]> getRowSet(RowIterator inputIterator) {
		List<Object[]> rowSet = new ArrayList<>();
		List<String> keyList = new ArrayList<>();
		while (inputIterator.hasNext()) {
			Row inputRow = inputIterator.nextRow();
			if (updateKeyList(inputRow, keyList)) {
				rowSet.add(getRow(inputRow));
			}
		}
		return rowSet;
	}

	private boolean updateKeyList(Row inputRow, List<String> keyList) {
		String key = genDistinctKey(inputRow);
		if (!keyList.contains(key)) {
			keyList.add(key);
			return true;
		}
		return false;
	}

	private String genDistinctKey(Row inputRow) {
		StringBuilder sb = new StringBuilder();
		for (Expression expression : expressions) {
			sb.append(expression.evaluate(inputRow));
		}
		return sb.toString();
	}

	private Object[] getRow(Row inputRow) {
		Object[] row = new Object[expressions.length];
		int columnIndex = 0;
		for (Expression expression : expressions) {
			row[columnIndex++] = expression.evaluate(inputRow);
		}
		return row;
	}

	public Distinct setInput(RelationProvider relation, Expression ... expressions ) {
		resetInput();
		this.relation = relation;
		this.expressions = expressions;
		addInput(relation);
		return this;
	}

	@Override
	public String toString() {
		return IndentPrint.print(this);
	}

}
