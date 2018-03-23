package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.Distinct;
import com.xingcloud.xa.session2.ra.Projection;
import com.xingcloud.xa.session2.ra.Relation;
import com.xingcloud.xa.session2.ra.RelationProvider;
import com.xingcloud.xa.session2.ra.Row;
import com.xingcloud.xa.session2.ra.RowIterator;
import com.xingcloud.xa.session2.ra.expr.AggregationExpr;
import com.xingcloud.xa.session2.ra.expr.ColumnValue;
import com.xingcloud.xa.session2.ra.expr.Constant;
import com.xingcloud.xa.session2.ra.expr.Expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author: mulisen
 * Date:   2/6/13
 */
public class XProjection extends AbstractOperation implements Projection{

	RelationProvider relation;
	Expression[]     projections;

	public Relation evaluate() {
		//0. check
		if (relation == null || projections == null) {
			return result;
		}

		//1. get columnMap &  expressions
		Map<String, Integer> originColumnMap = getOriginColumnMap();
		Map<String, Integer> columnMap = new HashMap<>();
		List<Expression> expressionList = new ArrayList<>();
		int columnIndex = 0;
		for (Expression expression : projections) {
			//expanded_query
			if (isSelectAll(expression)) {
				for (String columnName : originColumnMap.keySet()) {
					expressionList.add(new ColumnValue(columnName));
					columnMap.put(columnName, columnIndex++);
				}
			} else {
				expressionList.add(expression);
				columnMap.put(getColumnName(expression, columnIndex), columnIndex++);
			}
		}

		//2. handle with aggregation
		boolean hasAggregation = false;
		RowIterator iterator = relation.iterator();
		Row inputRow = iterator.nextRow();
		Object[] aggregationRow = new Object[expressionList.size()];
		columnIndex = 0;
		for (Expression expression : expressionList) {
			if (expression instanceof AggregationExpr) {
				hasAggregation = true;
			}
			aggregationRow[columnIndex++] = expression.evaluate(inputRow);
		}

		//3. get rows
		List<Object[]> rowSet;
		if (hasAggregation) {
			rowSet = new ArrayList<>();
			rowSet.add(aggregationRow);
		} else {
			RowIterator inputIterator = relation.iterator();
			rowSet = getRows(inputIterator, expressionList);
		}

		return new XRelation(columnMap, rowSet);
	}

	private Map<String, Integer> getOriginColumnMap() {
		RowIterator rowIterator = relation.iterator();
		Row inputRow = rowIterator.nextRow();
		if (inputRow != null) {
			return ((XRelation.XRow) inputRow).columnNames;
		}
		return new HashMap<>();
	}

	private boolean isSelectAll(Expression expression) {
		return expression instanceof ColumnValue && "*".equals(((ColumnValue) expression).columnName);
	}

	private String getColumnName(Expression expression, int columnIndex) {
		if (expression instanceof ColumnValue) {
			return ((ColumnValue) expression).columnName;
		}
		if (expression instanceof Constant) {
			return String.valueOf(((Constant) expression).value);
		}
		if (expression instanceof Distinct){

		}

		//TODO
		return "c" + columnIndex;
	}

	private List<Object[]> getRows(RowIterator rowIterator, List<Expression> expressionList) {
		List<Object[]> rowSet = new ArrayList<>();
		int rowLength = expressionList.size();
		while (rowIterator.hasNext()) {
			Row inputRow = rowIterator.nextRow();
			Object[] row = new Object[rowLength];
			int columnIndex = 0;
			for (Expression expression : expressionList) {
				row[columnIndex++] = expression.evaluate(inputRow);
			}
			rowSet.add(row);
		}
		return rowSet;
	}

	public Projection setInput(RelationProvider relation, Expression ... projections) {
		resetInput();
		this.relation = relation;
		this.projections = projections;
		addInput(relation);
		return this;
	}

	@Override
	public String toString() {
		return IndentPrint.print(this);
	}
}
