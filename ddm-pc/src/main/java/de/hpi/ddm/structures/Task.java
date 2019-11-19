package de.hpi.ddm.structures;

import java.io.Serializable;
import java.util.HashMap;

public class Task implements Serializable, Work {
    public String characterSet;
    public int start;
    public int end;
    public String missingChar;
}
