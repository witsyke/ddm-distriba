package de.hpi.ddm.structures;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@NoArgsConstructor
@AllArgsConstructor
public class Password implements Serializable {
    public String password;
    public List<String> hints;
}
