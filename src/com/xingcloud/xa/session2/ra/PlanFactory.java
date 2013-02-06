package com.xingcloud.xa.session2.ra;

import com.xingcloud.xa.session2.ra.impl.*;

import java.util.Hashtable;

/**
 * Author: mulisen
 * Date:   2/6/13
 */
public class PlanFactory {

	private static PlanFactory instance = new PlanFactory();
	private final Hashtable<String, Class<? extends Operation>> implementations;

	private PlanFactory(){
		implementations = new Hashtable<String, Class<? extends Operation>>();
		implementations.put("Distinct", XDistinct.class);
		implementations.put("Projection", XProjection.class);
		implementations.put("Selection", XSelection.class);
		implementations.put("Join", XJoin.class);
		implementations.put("GroupCount", XGroupCount.class);
		implementations.put("GroupSum", XGroupSum.class);
	}

	public static PlanFactory getInstance(){
		return instance;
	}

	public Operation createOpeation(Class<? extends Operation> clz){
		Class<? extends Operation> implementClz = implementations.get(clz.getSimpleName());
		try {
			return implementClz.newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();  //e:
		} catch (IllegalAccessException e) {
			e.printStackTrace();  //e:
		}
		return null;
	}

	public Distinct newDistinct(){
		return (Distinct) createOpeation(Distinct.class);
	}
	public GroupCount newGroupCount(){
		return (GroupCount) createOpeation(GroupCount.class);
	}

	public GroupSum newGroupSum(){
		return (GroupSum) createOpeation(GroupSum.class);
	}

	public Join newJoin(){
		return (Join) createOpeation(Join.class);
	}

	public Projection newProjection(){
		return (Projection) createOpeation(Projection.class);
	}

	public Selection newSelection(){
		return (Selection) createOpeation(Selection.class);
	}

	public TableScan newTableScan(String tableName){
		return new XTableScan(tableName);
	}

	public static void main(String[] args) {
		PlanFactory f = getInstance();

		//DAU
		//SELECT COUNT(DISTINCT(uid))
		// FROM event NATURAL JOIN user
		// WHERE user.register_time='2013-02-01'
		// AND event.date='2013-02-02' and event.event='visit'
		f.newGroupCount().setInput(
				f.newDistinct().setInput(
						f.newJoin().setInput(
								f.newSelection().setInput(
										f.newTableScan("user"),
										new XExpression("register_time='2013-02-01'")
								),
								f.newSelection().setInput(
										f.newTableScan("event"),
										new XExpression("date='2013-02-02' and event='visit'")
								)
						),
						"uid"
				)
				,null
		);
	}
}
