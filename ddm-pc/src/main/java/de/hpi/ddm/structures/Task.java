package de.hpi.ddm.structures;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashMap;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class Task implements Serializable, Work {
    public String characterSet;
    public int start;
    public int end;
    public String missingChar;
}
