package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.Relation;
import com.xingcloud.xa.session2.ra.RelationProvider;
import com.xingcloud.xa.session2.ra.Row;
import com.xingcloud.xa.session2.ra.RowIterator;
import com.xingcloud.xa.session2.ra.Selection;
import com.xingcloud.xa.session2.ra.expr.Expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Author: mulisen
 * Date:   2/6/13
 */
public class XSelection extends AbstractOperation implements Selection{

	RelationProvider relation;
	Expression expression;

	public Selection setInput(RelationProvider relation, Expression e) {
		resetInput();
		this.relation = relation;
		this.expression = e;
		addInput(relation);
		return this;
	}

	public Relation evaluate() {
		//1. get columnMap
		Map<String, Integer> columnMap = getOriginColumnMap();

		//2. get input
		RowIterator inputIterator = relation.iterator();

		//3. get rows
		List<Object[]> rowSet = getRowSet(inputIterator);

		return new XRelation(columnMap, rowSet);
	}

	private Map<String, Integer> getOriginColumnMap() {
		RowIterator rowIterator = relation.iterator();
		Row inputRow = rowIterator.nextRow();
		return ((XRelation.XRow) inputRow).columnNames;
	}

	private List<Object[]> getRowSet(RowIterator inputIterator) {
		List<Object[]> rowSet = new ArrayList<>();
		while (inputIterator.hasNext()) {
			XRelation.XRow inputRow = (XRelation.XRow) inputIterator.nextRow();
			if (expression == null || (Boolean) expression.evaluate(inputRow)) {
				rowSet.add(inputRow.rowData);
			}
		}
		return rowSet;
	}

	@Override
	public String toString() {
		return IndentPrint.print(this);
	}
}
