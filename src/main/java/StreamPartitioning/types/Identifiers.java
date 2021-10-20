package StreamPartitioning.types;

import org.apache.flink.statefun.sdk.FunctionType;

/**
 * Identifiers of stateful function namespaces
 * @param PART_TYPE Type of part function. Represents a part of partition that stores data
 *
 */
public class Identifiers {
    public static final FunctionType PART_TYPE = new FunctionType("partitioning", "part");
}