package com.elta.words.props;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;

/**
 * @author Evgeny Borisov
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Component
public class UserProperties implements Serializable {


    private Set<String> garbage;

    @Value("${garbage}")
    public void setGarbage(String[] garbage) {
        this.garbage = new HashSet<>(asList(garbage));
    }
}
