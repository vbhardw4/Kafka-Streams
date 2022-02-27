package com.github.kafkastreams.applications;

import lombok.*;

import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class Customer implements Serializable {
    private static final long serialVersionUID = 7526472295622776147L;
    int id;
    String customerName;
    String depositedAt;
    int amountDeposited;
}
