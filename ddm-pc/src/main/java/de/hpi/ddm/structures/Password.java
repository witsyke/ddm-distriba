package de.hpi.ddm.structures;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.util.List;

@NoArgsConstructor
@AllArgsConstructor
public class Password {
    private String password;
    private List<String> hints;
}
