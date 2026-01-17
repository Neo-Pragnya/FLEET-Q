#!/usr/bin/env python3
"""
Resource monitoring utility for FLEET-Q.

This script provides real-time monitoring of system resources during
FLEET-Q operation, including CPU usage, memory consumption, and
queue statistics.

Usage:
    python scripts/monitor_resources.py [options]

Options:
    --interval SECS    Monitoring interval in seconds (default: 5)
    --duration SECS    Total monitoring duration in seconds (default: 300)
    --output FILE      Output file for monitoring data (CSV format)
    --plot             Generate performance plots (requires matplotlib)
    --alert-cpu PCT    Alert when CPU usage exceeds percentage (default: 80)
    --alert-memory MB  Alert when memory usage exceeds MB (default: 1000)
"""

import argparse
import asyncio
import csv
import time
import psutil
import os
import sys
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fleet_q.config import FleetQConfig
from fleet_q.sqlite_simulation_storage import SQLiteSimulationStorage


@dataclass
class ResourceSample:
    """Container for resource monitoring sample."""
    timestamp: datetime
    cpu_percent: float
    memory_mb: float
    memory_percent: float
    disk_usage_percent: float
    network_bytes_sent: int
    network_bytes_recv: int
    process_count: int
    thread_count: int
    queue_stats: Dict[str, int]


class ResourceMonitor:
    """
    Real-time resource monitoring for FLEET-Q systems.
    
    Monitors system resources and queue statistics to provide
    insights into system performance and resource utilization.
    """
    
    def __init__(self, interval: int = 5, storage_path: Optional[str] = None):
        self.interval = interval
        self.storage_path = storage_path
        self.storage = None
        self.samples: List[ResourceSample] = []
        self.process = psutil.Process()
        self.start_time = None
        self.alerts_enabled = True
        self.alert_thresholds = {
            'cpu_percent': 80,
            'memory_mb': 1000
        }
    
    def set_alert_thresholds(self, cpu_percent: int = 80, memory_mb: int = 1000) -> None:
        """Set alert thresholds for resource monitoring."""
        self.alert_thresholds['cpu_percent'] = cpu_percent
        self.alert_thresholds['memory_mb'] = memory_mb
    
    async def initialize_storage(self) -> None:
        """Initialize storage connection for queue monitoring."""
        if self.storage_path:
            config = FleetQConfig(
                snowflake_account="monitor",
                snowflake_user="monitor",
                snowflake_password="monitor",
                snowflake_database="monitor",
                sqlite_db_path=self.storage_path
            )
            
            self.storage = SQLiteSimulationStorage(config)
            try:
                await self.storage.initialize()
            except Exception as e:
                print(f"⚠️  Could not connect to storage: {e}")
                self.storage = None
    
    async def collect_sample(self) -> ResourceSample:
        """Collect a single resource monitoring sample."""
        timestamp = datetime.now()
        
        # System-wide metrics
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        network = psutil.net_io_counters()
        
        # Process-specific metrics
        try:
            process_memory = self.process.memory_info().rss / 1024 / 1024  # MB
            process_cpu = self.process.cpu_percent()
            process_threads = self.process.num_threads()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            process_memory = 0
            process_cpu = 0
            process_threads = 0
        
        # Queue statistics (if storage available)
        queue_stats = {}
        if self.storage:
            try:
                stats = await self.storage.get_queue_stats()
                queue_stats = {
                    'pending_steps': stats.get('pending_steps', 0),
                    'claimed_steps': stats.get('claimed_steps', 0),
                    'completed_steps': stats.get('completed_steps', 0),
                    'failed_steps': stats.get('failed_steps', 0),
                    'active_pods': stats.get('active_pods', 0),
                    'total_steps': stats.get('total_steps', 0)
                }
            except Exception:
                queue_stats = {'error': 1}
        
        sample = ResourceSample(
            timestamp=timestamp,
            cpu_percent=cpu_percent,
            memory_mb=process_memory,
            memory_percent=memory.percent,
            disk_usage_percent=disk.percent,
            network_bytes_sent=network.bytes_sent,
            network_bytes_recv=network.bytes_recv,
            process_count=len(psutil.pids()),
            thread_count=process_threads,
            queue_stats=queue_stats
        )
        
        return sample
    
    def check_alerts(self, sample: ResourceSample) -> List[str]:
        """Check for alert conditions and return alert messages."""
        alerts = []
        
        if not self.alerts_enabled:
            return alerts
        
        # CPU alert
        if sample.cpu_percent > self.alert_thresholds['cpu_percent']:
            alerts.append(f"🚨 HIGH CPU: {sample.cpu_percent:.1f}% (threshold: {self.alert_thresholds['cpu_percent']}%)")
        
        # Memory alert
        if sample.memory_mb > self.alert_thresholds['memory_mb']:
            alerts.append(f"🚨 HIGH MEMORY: {sample.memory_mb:.1f}MB (threshold: {self.alert_thresholds['memory_mb']}MB)")
        
        # System memory alert
        if sample.memory_percent > 90:
            alerts.append(f"🚨 SYSTEM MEMORY: {sample.memory_percent:.1f}% used")
        
        # Disk space alert
        if sample.disk_usage_percent > 90:
            alerts.append(f"🚨 DISK SPACE: {sample.disk_usage_percent:.1f}% used")
        
        # Queue alerts (if available)
        if 'pending_steps' in sample.queue_stats:
            pending = sample.queue_stats['pending_steps']
            if pending > 1000:
                alerts.append(f"🚨 QUEUE BACKLOG: {pending} pending steps")
            
            failed = sample.queue_stats.get('failed_steps', 0)
            total = sample.queue_stats.get('total_steps', 1)
            failure_rate = failed / total if total > 0 else 0
            if failure_rate > 0.1:  # 10% failure rate
                alerts.append(f"🚨 HIGH FAILURE RATE: {failure_rate:.1%} ({failed}/{total})")
        
        return alerts
    
    def print_sample(self, sample: ResourceSample, show_details: bool = False) -> None:
        """Print a formatted resource sample."""
        elapsed = (sample.timestamp - self.start_time).total_seconds() if self.start_time else 0
        
        print(f"[{elapsed:6.0f}s] CPU: {sample.cpu_percent:5.1f}% | "
              f"Memory: {sample.memory_mb:6.1f}MB ({sample.memory_percent:4.1f}%) | "
              f"Threads: {sample.thread_count:3d}")
        
        if show_details and 'pending_steps' in sample.queue_stats:
            stats = sample.queue_stats
            print(f"         Queue: P:{stats['pending_steps']:3d} "
                  f"C:{stats['claimed_steps']:3d} "
                  f"✓:{stats['completed_steps']:4d} "
                  f"✗:{stats['failed_steps']:3d} "
                  f"Pods:{stats['active_pods']:2d}")
    
    async def monitor(self, duration: int = 300, verbose: bool = False) -> List[ResourceSample]:
        """
        Run resource monitoring for specified duration.
        
        Args:
            duration: Monitoring duration in seconds
            verbose: Show detailed output
            
        Returns:
            List of collected resource samples
        """
        print(f"🔍 Starting resource monitoring for {duration}s (interval: {self.interval}s)")
        
        if self.storage_path:
            await self.initialize_storage()
            if self.storage:
                print(f"📊 Queue monitoring enabled for: {self.storage_path}")
        
        self.start_time = datetime.now()
        end_time = time.time() + duration
        
        print(f"Alert thresholds: CPU {self.alert_thresholds['cpu_percent']}%, "
              f"Memory {self.alert_thresholds['memory_mb']}MB")
        print("-" * 80)
        
        try:
            while time.time() < end_time:
                sample = await self.collect_sample()
                self.samples.append(sample)
                
                # Check for alerts
                alerts = self.check_alerts(sample)
                for alert in alerts:
                    print(alert)
                
                # Print sample
                self.print_sample(sample, show_details=verbose)
                
                # Wait for next interval
                await asyncio.sleep(self.interval)
                
        except KeyboardInterrupt:
            print("\n⚠️  Monitoring interrupted by user")
        
        finally:
            if self.storage:
                await self.storage.close()
        
        print(f"\n✅ Monitoring complete. Collected {len(self.samples)} samples.")
        return self.samples
    
    def save_to_csv(self, output_file: str) -> None:
        """Save monitoring data to CSV file."""
        if not self.samples:
            print("No samples to save")
            return
        
        with open(output_file, 'w', newline='') as csvfile:
            # Flatten queue_stats for CSV
            fieldnames = [
                'timestamp', 'cpu_percent', 'memory_mb', 'memory_percent',
                'disk_usage_percent', 'network_bytes_sent', 'network_bytes_recv',
                'process_count', 'thread_count'
            ]
            
            # Add queue stats fields if available
            if self.samples[0].queue_stats:
                queue_fields = list(self.samples[0].queue_stats.keys())
                fieldnames.extend([f'queue_{field}' for field in queue_fields])
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for sample in self.samples:
                row = asdict(sample)
                
                # Flatten queue_stats
                queue_stats = row.pop('queue_stats', {})
                for key, value in queue_stats.items():
                    row[f'queue_{key}'] = value
                
                writer.writerow(row)
        
        print(f"📁 Monitoring data saved to: {output_file}")
    
    def generate_summary(self) -> Dict[str, Any]:
        """Generate summary statistics from collected samples."""
        if not self.samples:
            return {}
        
        cpu_values = [s.cpu_percent for s in self.samples]
        memory_values = [s.memory_mb for s in self.samples]
        
        summary = {
            'monitoring_duration_seconds': (self.samples[-1].timestamp - self.samples[0].timestamp).total_seconds(),
            'total_samples': len(self.samples),
            'cpu_stats': {
                'average': sum(cpu_values) / len(cpu_values),
                'min': min(cpu_values),
                'max': max(cpu_values)
            },
            'memory_stats': {
                'average_mb': sum(memory_values) / len(memory_values),
                'min_mb': min(memory_values),
                'max_mb': max(memory_values),
                'growth_mb': max(memory_values) - min(memory_values)
            }
        }
        
        # Add queue statistics if available
        if self.samples[0].queue_stats and 'pending_steps' in self.samples[0].queue_stats:
            pending_values = [s.queue_stats.get('pending_steps', 0) for s in self.samples]
            completed_values = [s.queue_stats.get('completed_steps', 0) for s in self.samples]
            
            summary['queue_stats'] = {
                'avg_pending': sum(pending_values) / len(pending_values),
                'max_pending': max(pending_values),
                'total_completed': max(completed_values) if completed_values else 0,
                'throughput_steps_per_sec': max(completed_values) / summary['monitoring_duration_seconds'] if completed_values and summary['monitoring_duration_seconds'] > 0 else 0
            }
        
        return summary
    
    def print_summary(self) -> None:
        """Print monitoring summary."""
        summary = self.generate_summary()
        
        if not summary:
            print("No data to summarize")
            return
        
        print(f"\n📈 Monitoring Summary:")
        print(f"  Duration: {summary['monitoring_duration_seconds']:.1f}s")
        print(f"  Samples: {summary['total_samples']}")
        
        cpu_stats = summary['cpu_stats']
        print(f"  CPU: avg {cpu_stats['average']:.1f}%, "
              f"min {cpu_stats['min']:.1f}%, "
              f"max {cpu_stats['max']:.1f}%")
        
        mem_stats = summary['memory_stats']
        print(f"  Memory: avg {mem_stats['average_mb']:.1f}MB, "
              f"min {mem_stats['min_mb']:.1f}MB, "
              f"max {mem_stats['max_mb']:.1f}MB, "
              f"growth {mem_stats['growth_mb']:.1f}MB")
        
        if 'queue_stats' in summary:
            queue_stats = summary['queue_stats']
            print(f"  Queue: avg pending {queue_stats['avg_pending']:.1f}, "
                  f"max pending {queue_stats['max_pending']}, "
                  f"completed {queue_stats['total_completed']}, "
                  f"throughput {queue_stats['throughput_steps_per_sec']:.2f} steps/sec")


def create_performance_plots(samples: List[ResourceSample], output_prefix: str = "performance") -> None:
    """Create performance plots from monitoring data."""
    try:
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
    except ImportError:
        print("⚠️  matplotlib not available. Install with: pip install matplotlib")
        return
    
    if not samples:
        print("No data to plot")
        return
    
    timestamps = [s.timestamp for s in samples]
    
    # Create subplots
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle('FLEET-Q Performance Monitoring', fontsize=16)
    
    # CPU Usage
    cpu_values = [s.cpu_percent for s in samples]
    ax1.plot(timestamps, cpu_values, 'b-', linewidth=2)
    ax1.set_title('CPU Usage')
    ax1.set_ylabel('CPU %')
    ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    
    # Memory Usage
    memory_values = [s.memory_mb for s in samples]
    ax2.plot(timestamps, memory_values, 'r-', linewidth=2)
    ax2.set_title('Memory Usage')
    ax2.set_ylabel('Memory (MB)')
    ax2.grid(True, alpha=0.3)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    
    # Thread Count
    thread_values = [s.thread_count for s in samples]
    ax3.plot(timestamps, thread_values, 'g-', linewidth=2)
    ax3.set_title('Thread Count')
    ax3.set_ylabel('Threads')
    ax3.grid(True, alpha=0.3)
    ax3.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    
    # Queue Statistics (if available)
    if samples[0].queue_stats and 'pending_steps' in samples[0].queue_stats:
        pending_values = [s.queue_stats.get('pending_steps', 0) for s in samples]
        completed_values = [s.queue_stats.get('completed_steps', 0) for s in samples]
        
        ax4.plot(timestamps, pending_values, 'orange', label='Pending', linewidth=2)
        ax4.plot(timestamps, completed_values, 'purple', label='Completed', linewidth=2)
        ax4.set_title('Queue Statistics')
        ax4.set_ylabel('Steps')
        ax4.legend()
        ax4.grid(True, alpha=0.3)
        ax4.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    else:
        # System Memory if no queue stats
        sys_memory_values = [s.memory_percent for s in samples]
        ax4.plot(timestamps, sys_memory_values, 'm-', linewidth=2)
        ax4.set_title('System Memory Usage')
        ax4.set_ylabel('Memory %')
        ax4.grid(True, alpha=0.3)
        ax4.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    
    # Rotate x-axis labels
    for ax in [ax1, ax2, ax3, ax4]:
        ax.tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    
    # Save plots
    plot_file = f"{output_prefix}_plots.png"
    plt.savefig(plot_file, dpi=300, bbox_inches='tight')
    print(f"📊 Performance plots saved to: {plot_file}")
    
    plt.close()


async def main():
    """Main entry point for resource monitoring."""
    parser = argparse.ArgumentParser(
        description="Monitor FLEET-Q system resources",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--interval',
        type=int,
        default=5,
        help='Monitoring interval in seconds (default: 5)'
    )
    
    parser.add_argument(
        '--duration',
        type=int,
        default=300,
        help='Total monitoring duration in seconds (default: 300)'
    )
    
    parser.add_argument(
        '--storage',
        help='Path to SQLite database for queue monitoring'
    )
    
    parser.add_argument(
        '--output',
        default='resource_monitoring.csv',
        help='Output file for monitoring data (default: resource_monitoring.csv)'
    )
    
    parser.add_argument(
        '--plot',
        action='store_true',
        help='Generate performance plots (requires matplotlib)'
    )
    
    parser.add_argument(
        '--alert-cpu',
        type=int,
        default=80,
        help='Alert when CPU usage exceeds percentage (default: 80)'
    )
    
    parser.add_argument(
        '--alert-memory',
        type=int,
        default=1000,
        help='Alert when memory usage exceeds MB (default: 1000)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Show detailed queue statistics'
    )
    
    args = parser.parse_args()
    
    # Create monitor
    monitor = ResourceMonitor(interval=args.interval, storage_path=args.storage)
    monitor.set_alert_thresholds(cpu_percent=args.alert_cpu, memory_mb=args.alert_memory)
    
    try:
        # Run monitoring
        samples = await monitor.monitor(duration=args.duration, verbose=args.verbose)
        
        # Save data
        monitor.save_to_csv(args.output)
        
        # Print summary
        monitor.print_summary()
        
        # Generate plots if requested
        if args.plot:
            create_performance_plots(samples, output_prefix=args.output.replace('.csv', ''))
        
    except KeyboardInterrupt:
        print("\n⚠️  Monitoring interrupted by user")
    except Exception as e:
        print(f"\n💥 Monitoring error: {e}")


if __name__ == "__main__":
    asyncio.run(main())