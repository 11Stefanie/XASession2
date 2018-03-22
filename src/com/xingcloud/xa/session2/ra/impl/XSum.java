package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.Aggregation;
import com.xingcloud.xa.session2.ra.RelationProvider;
import com.xingcloud.xa.session2.ra.Row;
import com.xingcloud.xa.session2.ra.RowIterator;
import com.xingcloud.xa.session2.ra.Sum;

/**
 * Author: mulisen
 * Date:   2/7/13
 */
public class XSum extends AbstractAggregation implements Sum {

	RelationProvider relation;
	String columnName;
	public Aggregation setInput(RelationProvider relation, String columnName) {
		resetInput();
		init();
		this.relation = relation;
		this.columnName = columnName;
		addInput(relation);
		return this;
	}

	public Object aggregate() {
		long sum = 0L;
		RowIterator rowIterator = relation.iterator();
		while (rowIterator.hasNext()) {
			Row row = rowIterator.nextRow();
			sum += Long.valueOf(String.valueOf(row.get(columnName)));
		}
		return String.valueOf(sum);
	}

	public void init() {
		//TODO method implementation
	}

}
