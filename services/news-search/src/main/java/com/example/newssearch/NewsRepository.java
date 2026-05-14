package com.example.newssearch;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import java.util.List;

public interface NewsRepository extends JpaRepository<NewsArticle, Long> {
    List<NewsArticle> findByTitleContainingIgnoreCaseOrPressContainingIgnoreCaseOrderByCreatedAtDesc(String title, String press);

    @Query("SELECT n FROM NewsArticle n WHERE LOWER(n.title) LIKE LOWER(CONCAT('%', :query, '%')) OR LOWER(n.press) LIKE LOWER(CONCAT('%', :query, '%')) ORDER BY n.createdAt DESC")
    List<NewsArticle> searchNews(@Param("query") String query);

    @Query(value = "SELECT * FROM news_history ORDER BY created_at DESC LIMIT :limit", nativeQuery = true)
    List<NewsArticle> findLatest(@Param("limit") int limit);
}
