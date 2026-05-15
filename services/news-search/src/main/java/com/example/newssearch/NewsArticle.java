package com.example.newssearch;

import jakarta.persistence.*;
import lombok.Data;
import java.time.LocalDateTime;

@Entity
@Table(name = "news_history")
@Data
public class NewsArticle {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String press;
    private String title;
    private String link;
    private String author;
    private String content;
    private String thumbnail;

    @Column(name = "created_at")
    private LocalDateTime createdAt;
}
