package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.Join;
import com.xingcloud.xa.session2.ra.Relation;
import com.xingcloud.xa.session2.ra.RelationProvider;
import com.xingcloud.xa.session2.ra.Row;
import com.xingcloud.xa.session2.ra.RowIterator;
import com.xingcloud.xa.session2.ra.expr.Expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author: mulisen
 * Date:   2/6/13
 */
public class XJoin extends AbstractOperation implements Join{

	RelationProvider left;
	RelationProvider right;

	public Relation evaluate() {
		//1. get columnMap
		Map<Integer, Integer> sameColumnIndexMap = new HashMap<>();
		Map<String, Integer> columnMap = getColumnMap(sameColumnIndexMap);

		//2. get input
		RowIterator leftIterator = left.iterator();
		RowIterator rightIterator = right.iterator();

		//3. get rows
		List<Object[]> rowSet = getRowSet(leftIterator, rightIterator, columnMap, sameColumnIndexMap);
		return new XRelation(columnMap, rowSet);
	}

	//joinColumnMap.<k,v> : <rightColumnIndex, leftColumnIndex>
	private Map<String, Integer> getColumnMap(Map<Integer, Integer> joinColumnMap) {
		Map<String, Integer> columnMap = new HashMap<>();
		Map<String, Integer> leftColumnMap = left.getColumnIndex();
		Map<String, Integer> rightColumnMap = right.getColumnIndex();

		//1.add leftColumnMap
		columnMap.putAll(leftColumnMap);
		int columnIndex = leftColumnMap.size();
		for (String columnName : rightColumnMap.keySet()) {
			Integer leftColumnIndex = columnMap.get(columnName);
			if (leftColumnIndex != null) {
				//2. if same, update joinColumnMap
				int rightColumnIndex = rightColumnMap.get(columnName);
				joinColumnMap.put(rightColumnIndex, leftColumnIndex);
			} else {
				//3. if not same, add rightColumn(Name&newIndex)
				columnMap.put(columnName, columnIndex++);
			}
		}
		return columnMap;
	}

	private List<Object[]> getRowSet(RowIterator leftIterator, RowIterator rightIterator, Map<String, Integer> columnMap,
																	 Map<Integer, Integer> sameColumnIndexMap) {
		List<Object[]> rowSet = new ArrayList();
		List<Object[]> rightRows = getRows(rightIterator);
		while (leftIterator.hasNext()) {
			Row leftRow = leftIterator.nextRow();
			Object[] row = new Object[columnMap.size()];
			//1. add leftRowData
			addRowData(row, leftRow);
			for (Object[] rightRow : rightRows) {
				//2. add rightRowData if can be joined
				if (isJoinRow(leftRow, rightRow, sameColumnIndexMap)) {
					addRightRowData(row, rightRow, columnMap, sameColumnIndexMap);
					rowSet.add(row);
				}
			}
		}
		return rowSet;
	}

	// get row list from RowIterator
	private List<Object[]> getRows(RowIterator rowIterator) {
		List<Object[]> rows = new ArrayList<>();
		while (rowIterator.hasNext()) {
			XRelation.XRow inputRow = (XRelation.XRow) rowIterator.nextRow();
			rows.add(inputRow.rowData);
		}
		return rows;
	}

	// add inputRow.rowData to row
	private void addRowData(Object[] row, Row inputRow) {
		XRelation.XRow xRow = (XRelation.XRow) inputRow;
		int columnIndex = 0;
		for (Object data : xRow.rowData) {
			row[columnIndex++] = data;
		}
	}

	// add inputRow.rowData to row——for rightRow
	private void addRightRowData(Object[] row, Object[] inputRow, Map<String, Integer> columnMap,
															 Map<Integer, Integer> sameColumnIndexMap) {
		Map<String, Integer> rightColumnMap = right.getColumnIndex();

		for (String columnName : rightColumnMap.keySet()) {
			int rightIndex = rightColumnMap.get(columnName);
			int columnIndex = columnMap.get(columnName);
			if (sameColumnIndexMap.get(rightIndex) == null) {
				row[columnIndex] = inputRow[rightIndex];
			}
		}
	}

	//check if both leftRow and rightRow can be joined——if(leftRow.joinColumnValumn == rightRow.joinColumnValumn)
	private boolean isJoinRow(Row leftRow, Object[] rightRowArray, Map<Integer, Integer> sameColumnIndexMap) {
		XRelation.XRow leftXRow = (XRelation.XRow) leftRow;
		StringBuilder leftJoinKeySb = new StringBuilder();
		StringBuilder rightJoinKeySb = new StringBuilder();
		for (Integer rightColumnIndex : sameColumnIndexMap.keySet()) {
			leftJoinKeySb.append(leftXRow.rowData[sameColumnIndexMap.get(rightColumnIndex)]);
			rightJoinKeySb.append(rightRowArray[rightColumnIndex]);
		}
		return leftJoinKeySb.toString().equals(rightJoinKeySb.toString());
	}

	public Join setInput(RelationProvider left, RelationProvider right) {
		resetInput();
		this.left = left;
		this.right = right;
		addInput(left);
		addInput(right);
		return this;
	}

	@Override
	public String toString() {
		return IndentPrint.print(this);
	}

}
