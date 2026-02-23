from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List
from collections import defaultdict


@dataclass
class Metrics:
    """Application metrics tracker"""
    
    # Counters
    total_requests: int = 0
    mongo_requests: int = 0
    mysql_requests: int = 0
    errors: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    
    # Quality tracking
    total_quality: int = 0
    quality_count: int = 0
    quality_by_db: Dict[str, List[int]] = field(default_factory=lambda: defaultdict(list))
    
    # Timing
    total_response_time: float = 0.0
    response_count: int = 0
    
    # Error tracking
    error_types: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    
    # Start time
    start_time: datetime = field(default_factory=datetime.now)
    
    def record_request(self, db_type: str):
        """Record a new request"""
        self.total_requests += 1
        if db_type == "mongo":
            self.mongo_requests += 1
        else:
            self.mysql_requests += 1
    
    def record_cache_hit(self):
        """Record cache hit"""
        self.cache_hits += 1
    
    def record_cache_miss(self):
        """Record cache miss"""
        self.cache_misses += 1
    
    def record_quality(self, quality: int, db_type: str):
        """Record quality metric"""
        self.total_quality += quality
        self.quality_count += 1
        self.quality_by_db[db_type].append(quality)
    
    def record_response_time(self, duration: float):
        """Record response time"""
        self.total_response_time += duration
        self.response_count += 1
    
    def record_error(self, error_type: str):
        """Record error"""
        self.errors += 1
        self.error_types[error_type] += 1
    
    def get_summary(self) -> dict:
        """Get metrics summary"""
        uptime = (datetime.now() - self.start_time).total_seconds()
        
        avg_quality = (
            self.total_quality / self.quality_count 
            if self.quality_count > 0 else 0
        )
        
        avg_response_time = (
            self.total_response_time / self.response_count
            if self.response_count > 0 else 0
        )
        
        cache_hit_rate = (
            self.cache_hits / (self.cache_hits + self.cache_misses)
            if (self.cache_hits + self.cache_misses) > 0 else 0
        )
        
        error_rate = (
            self.errors / self.total_requests
            if self.total_requests > 0 else 0
        )
        
        # Calculate per-DB quality
        db_quality = {}
        for db_type, qualities in self.quality_by_db.items():
            if qualities:
                db_quality[db_type] = {
                    "avg": sum(qualities) / len(qualities),
                    "count": len(qualities),
                    "min": min(qualities),
                    "max": max(qualities)
                }
        
        return {
            "uptime_seconds": round(uptime, 2),
            "requests": {
                "total": self.total_requests,
                "mongo": self.mongo_requests,
                "mysql": self.mysql_requests,
            },
            "cache": {
                "hits": self.cache_hits,
                "misses": self.cache_misses,
                "hit_rate": round(cache_hit_rate, 4),
            },
            "quality": {
                "average": round(avg_quality, 2),
                "count": self.quality_count,
                "by_database": db_quality,
            },
            "performance": {
                "avg_response_time_ms": round(avg_response_time * 1000, 2),
                "total_responses": self.response_count,
            },
            "errors": {
                "total": self.errors,
                "rate": round(error_rate, 4),
                "by_type": dict(self.error_types),
            }
        }