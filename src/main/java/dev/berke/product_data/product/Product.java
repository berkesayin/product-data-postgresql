package dev.berke.product_data.product;

import dev.berke.product_data.category.Category;
import jakarta.persistence.*;
import lombok.*;

import java.math.BigDecimal;
import java.time.Instant;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Getter
@Setter
@Entity
@Table(name = "products")
@EqualsAndHashCode(of = {"productId"})
public class Product {

    @Id
    // @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "product_id", nullable = false, updatable = false)
    private Integer productId;

    @Column(name = "product_name", nullable = false)
    private String productName;

    @Column(name = "base_price", precision = 10, scale = 2)
    private BigDecimal basePrice;

    @Column(name = "min_price", precision = 10, scale = 2)
    private BigDecimal minPrice;

    @Column(name = "manufacturer")
    private String manufacturer;

    @Column(name = "sku", unique = true, nullable = false)
    private String sku;

    @Column(name = "created_on", nullable = false, updatable = false)
    private Instant createdOn;

    @Column(name = "status")
    private Boolean status;

    @ManyToOne(optional = false)
    @JoinColumn(
            name = "category_id", 
            referencedColumnName = "category_id", 
            nullable = false
    )
    private Category category;

    @PrePersist
    protected void onCreate() {
        if (createdOn == null) {
            createdOn = Instant.now();
        }
    }
}