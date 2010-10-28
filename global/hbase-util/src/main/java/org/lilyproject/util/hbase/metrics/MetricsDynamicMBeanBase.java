// Do not apply Lily license
package org.lilyproject.util.hbase.metrics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.ReflectionException;

import org.apache.hadoop.hbase.metrics.MetricsRate;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.util.*;

/**
 * This class was copied from Hadoop and extended to support some new types of metrics.
 */
public abstract class MetricsDynamicMBeanBase implements DynamicMBean {
  private final static String AVG_TIME = "AvgTime";
  private final static String MIN_TIME = "MinTime";
  private final static String MAX_TIME = "MaxTime";
  private final static String NUM_OPS = "NumOps";
  private final static String RESET_ALL_MIN_MAX_OP = "resetAllMinMax";
  private MetricsRegistry metricsRegistry;
  private MBeanInfo mbeanInfo;
  private Map<String, MetricsBase> metricsRateAttributeMod;
  private int numEntriesInRegistry = 0;
  private String mbeanDescription;

  protected MetricsDynamicMBeanBase(final MetricsRegistry mr, final String aMBeanDescription) {
    metricsRegistry = mr;
    mbeanDescription = aMBeanDescription;
    createMBeanInfo();
  }

  private void updateMbeanInfoIfMetricsListChanged()  {
    if (numEntriesInRegistry != metricsRegistry.size())
      createMBeanInfo();
  }

  private void createMBeanInfo() {
    metricsRateAttributeMod = new HashMap<String, MetricsBase>();
    boolean needsMinMaxResetOperation = false;
    List<MBeanAttributeInfo> attributesInfo = new ArrayList<MBeanAttributeInfo>();
    MBeanOperationInfo[] operationsInfo = null;
    numEntriesInRegistry = metricsRegistry.size();

    for (MetricsBase o : metricsRegistry.getMetricsList()) {

      if (MetricsTimeVaryingRate.class.isInstance(o)) {
        // For each of the metrics there are 3 different attributes
        attributesInfo.add(new MBeanAttributeInfo(o.getName() + NUM_OPS, "java.lang.Integer",
            o.getDescription(), true, false, false));
        attributesInfo.add(new MBeanAttributeInfo(o.getName() + AVG_TIME, "java.lang.Long",
            o.getDescription(), true, false, false));
        attributesInfo.add(new MBeanAttributeInfo(o.getName() + MIN_TIME, "java.lang.Long",
            o.getDescription(), true, false, false));
        attributesInfo.add(new MBeanAttributeInfo(o.getName() + MAX_TIME, "java.lang.Long",
            o.getDescription(), true, false, false));
        needsMinMaxResetOperation = true;  // the min and max can be reset.

        // Note the special attributes (AVG_TIME, MIN_TIME, ..) are derived from metrics
        // Rather than check for the suffix we store them in a map.
        metricsRateAttributeMod.put(o.getName() + NUM_OPS, o);
        metricsRateAttributeMod.put(o.getName() + AVG_TIME, o);
        metricsRateAttributeMod.put(o.getName() + MIN_TIME, o);
        metricsRateAttributeMod.put(o.getName() + MAX_TIME, o);

      }  else if ( MetricsIntValue.class.isInstance(o) || MetricsTimeVaryingInt.class.isInstance(o) ) {
        attributesInfo.add(new MBeanAttributeInfo(o.getName(), "java.lang.Integer",
            o.getDescription(), true, false, false));
      } else if ( MetricsLongValue.class.isInstance(o) || MetricsTimeVaryingLong.class.isInstance(o) ) {
        attributesInfo.add(new MBeanAttributeInfo(o.getName(), "java.lang.Long",
            o.getDescription(), true, false, false));
      } else if ( MetricsNonTimeRate.class.isInstance(o)) {
          attributesInfo.add(new MBeanAttributeInfo(o.getName(), "java.lang.Float", o.getDescription(),
                  true, false, false));
      } else if ( MetricsRate.class.isInstance(o)) {
          attributesInfo.add(new MBeanAttributeInfo(o.getName(), "java.lang.Float", o.getDescription(),
                  true, false, false));
      } else {
        MetricsUtil.LOG.error("unknown metrics type: " + o.getClass().getName());
      }

      if (needsMinMaxResetOperation) {
        operationsInfo = new MBeanOperationInfo[] {
            new MBeanOperationInfo(RESET_ALL_MIN_MAX_OP, "Reset (zero) All Min Max",
                    null, "void", MBeanOperationInfo.ACTION) };
      }
    }
    MBeanAttributeInfo[] attrArray = new MBeanAttributeInfo[attributesInfo.size()];
    mbeanInfo =  new MBeanInfo(this.getClass().getName(), mbeanDescription,
        attributesInfo.toArray(attrArray), null, operationsInfo, null);
  }

  public Object getAttribute(String attributeName) throws AttributeNotFoundException,
      MBeanException, ReflectionException {
    if (attributeName == null || attributeName.equals(""))
      throw new IllegalArgumentException();

    updateMbeanInfoIfMetricsListChanged();

    Object o = metricsRateAttributeMod.get(attributeName);
    if (o == null) {
      o = metricsRegistry.get(attributeName);
    }
    if (o == null)
      throw new AttributeNotFoundException();

    if (o instanceof MetricsIntValue)
      return ((MetricsIntValue) o).get();
    else if (o instanceof MetricsLongValue)
      return ((MetricsLongValue) o).get();
    else if (o instanceof MetricsTimeVaryingInt)
      return ((MetricsTimeVaryingInt) o).getPreviousIntervalValue();
    else if (o instanceof MetricsTimeVaryingLong)
      return ((MetricsTimeVaryingLong) o).getPreviousIntervalValue();
    else if (o instanceof MetricsTimeVaryingRate) {
      MetricsTimeVaryingRate or = (MetricsTimeVaryingRate) o;
      if (attributeName.endsWith(NUM_OPS))
        return or.getPreviousIntervalNumOps();
      else if (attributeName.endsWith(AVG_TIME))
        return or.getPreviousIntervalAverageTime();
      else if (attributeName.endsWith(MIN_TIME))
        return or.getMinTime();
      else if (attributeName.endsWith(MAX_TIME))
        return or.getMaxTime();
      else {
        MetricsUtil.LOG.error("Unexpected attrubute suffix");
        throw new AttributeNotFoundException();
      }
    } else if (o instanceof MetricsNonTimeRate) {
        MetricsNonTimeRate metric = (MetricsNonTimeRate)o;
        return metric.getPreviousIntervalValue();
    } else if (o instanceof MetricsRate) {
        MetricsRate metric = (MetricsRate)o;
        return metric.getPreviousIntervalValue();
    } else {
        MetricsUtil.LOG.error("unknown metrics type: " + o.getClass().getName());
        throw new AttributeNotFoundException();
    }
  }

  public AttributeList getAttributes(String[] attributeNames) {
    if (attributeNames == null || attributeNames.length == 0)
      throw new IllegalArgumentException();

    updateMbeanInfoIfMetricsListChanged();

    AttributeList result = new AttributeList(attributeNames.length);
    for (String iAttributeName : attributeNames) {
      try {
        Object value = getAttribute(iAttributeName);
        result.add(new Attribute(iAttributeName, value));
      } catch (Exception e) {
        continue;
      }
    }
    return result;
  }

  public MBeanInfo getMBeanInfo() {
    return mbeanInfo;
  }

  public Object invoke(String actionName, Object[] parms, String[] signature)
      throws MBeanException, ReflectionException {

    if (actionName == null || actionName.equals(""))
      throw new IllegalArgumentException();


    // Right now we support only one fixed operation (if it applies)
    if (!(actionName.equals(RESET_ALL_MIN_MAX_OP)) ||
        mbeanInfo.getOperations().length != 1) {
      throw new ReflectionException(new NoSuchMethodException(actionName));
    }
    for (MetricsBase m : metricsRegistry.getMetricsList())  {
      if ( MetricsTimeVaryingRate.class.isInstance(m) ) {
        MetricsTimeVaryingRate.class.cast(m).resetMinMax();
      }
    }
    return null;
  }

  public void setAttribute(Attribute attribute)
      throws AttributeNotFoundException, InvalidAttributeValueException,
      MBeanException, ReflectionException {
    throw new ReflectionException(new NoSuchMethodException("set" + attribute));
  }

  public AttributeList setAttributes(AttributeList attributes) {
    return null;
  }
}
