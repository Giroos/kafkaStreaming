package il.co.iai.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
@AllArgsConstructor
public class NavError implements Serializable {

    private Double x;
    private Double y;
    private Double z;
    private Double total;

}
