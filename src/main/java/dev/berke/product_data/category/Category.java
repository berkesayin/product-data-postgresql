package dev.berke.product_data.category;

import dev.berke.product_data.product.Product;
import jakarta.persistence.*;
import lombok.*;

import java.util.HashSet;
import java.util.Set;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Getter
@Setter
@Entity
@Table(name = "categories")

@ToString(exclude = "products") 
@EqualsAndHashCode(of = {"categoryId"}) 
public class Category {

    @Id
    // @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "category_id", nullable = false, updatable = false)
    private Integer categoryId;

    @Column(name = "category_name", nullable = false, unique = true)
    private String categoryName;

    @OneToMany(mappedBy = "category", cascade = {CascadeType.PERSIST, CascadeType.MERGE})
    private Set<Product> products = new HashSet<>(); 
}