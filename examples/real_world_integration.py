#!/usr/bin/env python3
"""
Real-World Integration Example

This example demonstrates a complete real-world scenario using FLEET-Q
for processing data pipeline tasks in a financial services environment.

Scenario: Daily financial report generation system
- Ingest data from multiple sources
- Process and validate financial data
- Generate reports in multiple formats
- Send notifications to stakeholders
- Handle failures and retries gracefully
"""

import asyncio
import json
import httpx
from datetime import datetime, date
from typing import Dict, Any, List, Optional
from pathlib import Path
import structlog

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


class FinancialDataPipeline:
    """
    Financial data processing pipeline using FLEET-Q
    
    This demonstrates a real-world use case where FLEET-Q coordinates
    multiple steps in a financial data processing workflow.
    """
    
    def __init__(self, fleet_q_base_url: str = "http://localhost:8000"):
        self.client = httpx.AsyncClient(base_url=fleet_q_base_url)
        self.pipeline_id = f"financial_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    async def submit_step(self, step_type: str, args: Dict[str, Any], 
                         priority: int = 0, max_retries: int = 3) -> str:
        """Submit a step to FLEET-Q"""
        
        payload = {
            "type": step_type,
            "args": args,
            "metadata": {
                "pipeline_id": self.pipeline_id,
                "submitted_at": datetime.utcnow().isoformat(),
                "environment": "production"
            },
            "priority": priority,
            "max_retries": max_retries
        }
        
        response = await self.client.post("/submit", json=payload)
        response.raise_for_status()
        
        step_data = response.json()
        step_id = step_data["step_id"]
        
        logger.info(
            "Step submitted to FLEET-Q",
            step_id=step_id,
            step_type=step_type,
            pipeline_id=self.pipeline_id
        )
        
        return step_id
    
    async def wait_for_step(self, step_id: str, timeout_seconds: int = 300) -> Dict[str, Any]:
        """Wait for a step to complete"""
        
        start_time = asyncio.get_event_loop().time()
        
        while True:
            response = await self.client.get(f"/status/{step_id}")
            response.raise_for_status()
            
            status_data = response.json()
            status = status_data["status"]
            
            if status in ["completed", "failed"]:
                logger.info(
                    "Step completed",
                    step_id=step_id,
                    status=status,
                    retry_count=status_data.get("retry_count", 0)
                )
                return status_data
            
            # Check timeout
            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed > timeout_seconds:
                raise TimeoutError(f"Step {step_id} did not complete within {timeout_seconds} seconds")
            
            await asyncio.sleep(2)
    
    async def run_daily_pipeline(self, report_date: date) -> Dict[str, Any]:
        """
        Run the complete daily financial reporting pipeline
        
        Pipeline steps:
        1. Data ingestion from multiple sources
        2. Data validation and cleansing
        3. Risk calculations
        4. Report generation
        5. Stakeholder notifications
        """
        
        pipeline_start = datetime.utcnow()
        report_date_str = report_date.strftime("%Y-%m-%d")
        
        logger.info(
            "Starting daily financial pipeline",
            pipeline_id=self.pipeline_id,
            report_date=report_date_str
        )
        
        try:
            # Step 1: Data Ingestion (parallel)
            ingestion_steps = await self._run_data_ingestion(report_date_str)
            
            # Step 2: Data Validation (depends on ingestion)
            validation_step = await self._run_data_validation(report_date_str, ingestion_steps)
            
            # Step 3: Risk Calculations (depends on validation)
            risk_step = await self._run_risk_calculations(report_date_str, validation_step)
            
            # Step 4: Report Generation (parallel, depends on risk)
            report_steps = await self._run_report_generation(report_date_str, risk_step)
            
            # Step 5: Notifications (depends on reports)
            notification_step = await self._run_notifications(report_date_str, report_steps)
            
            pipeline_end = datetime.utcnow()
            duration = (pipeline_end - pipeline_start).total_seconds()
            
            result = {
                "pipeline_id": self.pipeline_id,
                "status": "completed",
                "report_date": report_date_str,
                "duration_seconds": duration,
                "steps_completed": {
                    "ingestion": len(ingestion_steps),
                    "validation": 1,
                    "risk_calculation": 1,
                    "reports": len(report_steps),
                    "notifications": 1
                },
                "completed_at": pipeline_end.isoformat()
            }
            
            logger.info(
                "Daily financial pipeline completed successfully",
                **result
            )
            
            return result
            
        except Exception as e:
            logger.error(
                "Daily financial pipeline failed",
                pipeline_id=self.pipeline_id,
                report_date=report_date_str,
                error=str(e),
                exc_info=True
            )
            raise
    
    async def _run_data_ingestion(self, report_date: str) -> List[str]:
        """Run parallel data ingestion from multiple sources"""
        
        logger.info("Starting data ingestion phase", report_date=report_date)
        
        # Define data sources
        data_sources = [
            {"name": "trading_system", "priority": 10, "timeout": 300},
            {"name": "risk_system", "priority": 10, "timeout": 300},
            {"name": "market_data", "priority": 8, "timeout": 180},
            {"name": "reference_data", "priority": 5, "timeout": 120},
            {"name": "external_feeds", "priority": 3, "timeout": 600}
        ]
        
        # Submit ingestion steps in parallel
        step_ids = []
        for source in data_sources:
            step_id = await self.submit_step(
                step_type="data_ingestion",
                args={
                    "source": source["name"],
                    "report_date": report_date,
                    "timeout_seconds": source["timeout"]
                },
                priority=source["priority"],
                max_retries=3
            )
            step_ids.append(step_id)
        
        # Wait for all ingestion steps to complete
        completed_steps = []
        for step_id in step_ids:
            try:
                status = await self.wait_for_step(step_id, timeout_seconds=900)
                if status["status"] == "completed":
                    completed_steps.append(step_id)
                else:
                    logger.error(
                        "Data ingestion step failed",
                        step_id=step_id,
                        status=status
                    )
            except Exception as e:
                logger.error(
                    "Data ingestion step timeout or error",
                    step_id=step_id,
                    error=str(e)
                )
        
        logger.info(
            "Data ingestion phase completed",
            total_sources=len(data_sources),
            successful_sources=len(completed_steps),
            failed_sources=len(data_sources) - len(completed_steps)
        )
        
        if len(completed_steps) < len(data_sources) * 0.8:  # Require 80% success
            raise Exception("Insufficient data sources completed successfully")
        
        return completed_steps
    
    async def _run_data_validation(self, report_date: str, ingestion_steps: List[str]) -> str:
        """Run data validation and cleansing"""
        
        logger.info("Starting data validation phase", report_date=report_date)
        
        step_id = await self.submit_step(
            step_type="data_validation",
            args={
                "report_date": report_date,
                "input_steps": ingestion_steps,
                "validation_rules": [
                    "completeness_check",
                    "data_quality_check",
                    "business_rules_validation",
                    "cross_reference_validation"
                ]
            },
            priority=15,  # High priority
            max_retries=2
        )
        
        status = await self.wait_for_step(step_id, timeout_seconds=600)
        
        if status["status"] != "completed":
            raise Exception(f"Data validation failed: {status}")
        
        logger.info("Data validation phase completed", step_id=step_id)
        return step_id
    
    async def _run_risk_calculations(self, report_date: str, validation_step: str) -> str:
        """Run risk calculations"""
        
        logger.info("Starting risk calculations phase", report_date=report_date)
        
        step_id = await self.submit_step(
            step_type="risk_calculation",
            args={
                "report_date": report_date,
                "validation_step": validation_step,
                "calculations": [
                    "value_at_risk",
                    "expected_shortfall",
                    "stress_testing",
                    "scenario_analysis",
                    "credit_risk_metrics"
                ],
                "confidence_levels": [0.95, 0.99],
                "time_horizons": [1, 10, 250]  # days
            },
            priority=20,  # Very high priority
            max_retries=2
        )
        
        status = await self.wait_for_step(step_id, timeout_seconds=1200)  # 20 minutes
        
        if status["status"] != "completed":
            raise Exception(f"Risk calculations failed: {status}")
        
        logger.info("Risk calculations phase completed", step_id=step_id)
        return step_id
    
    async def _run_report_generation(self, report_date: str, risk_step: str) -> List[str]:
        """Generate multiple reports in parallel"""
        
        logger.info("Starting report generation phase", report_date=report_date)
        
        # Define reports to generate
        reports = [
            {
                "name": "executive_summary",
                "format": "pdf",
                "priority": 15,
                "recipients": ["ceo@company.com", "cfo@company.com"]
            },
            {
                "name": "risk_dashboard",
                "format": "html",
                "priority": 12,
                "recipients": ["risk-team@company.com"]
            },
            {
                "name": "regulatory_report",
                "format": "xml",
                "priority": 18,
                "recipients": ["compliance@company.com"]
            },
            {
                "name": "detailed_analytics",
                "format": "excel",
                "priority": 8,
                "recipients": ["analytics-team@company.com"]
            }
        ]
        
        # Submit report generation steps
        step_ids = []
        for report in reports:
            step_id = await self.submit_step(
                step_type="report_generation",
                args={
                    "report_date": report_date,
                    "risk_step": risk_step,
                    "report_name": report["name"],
                    "format": report["format"],
                    "recipients": report["recipients"]
                },
                priority=report["priority"],
                max_retries=2
            )
            step_ids.append(step_id)
        
        # Wait for all reports to complete
        completed_reports = []
        for step_id in step_ids:
            try:
                status = await self.wait_for_step(step_id, timeout_seconds=900)
                if status["status"] == "completed":
                    completed_reports.append(step_id)
                else:
                    logger.error(
                        "Report generation failed",
                        step_id=step_id,
                        status=status
                    )
            except Exception as e:
                logger.error(
                    "Report generation timeout or error",
                    step_id=step_id,
                    error=str(e)
                )
        
        logger.info(
            "Report generation phase completed",
            total_reports=len(reports),
            successful_reports=len(completed_reports)
        )
        
        return completed_reports
    
    async def _run_notifications(self, report_date: str, report_steps: List[str]) -> str:
        """Send notifications to stakeholders"""
        
        logger.info("Starting notifications phase", report_date=report_date)
        
        step_id = await self.submit_step(
            step_type="stakeholder_notification",
            args={
                "report_date": report_date,
                "report_steps": report_steps,
                "notification_types": [
                    "email_summary",
                    "slack_notification",
                    "dashboard_update"
                ],
                "stakeholder_groups": [
                    "executives",
                    "risk_managers",
                    "compliance_team",
                    "analytics_team"
                ]
            },
            priority=10,
            max_retries=3
        )
        
        status = await self.wait_for_step(step_id, timeout_seconds=300)
        
        if status["status"] != "completed":
            logger.warning(f"Notifications partially failed: {status}")
            # Don't fail the entire pipeline for notification issues
        
        logger.info("Notifications phase completed", step_id=step_id)
        return step_id
    
    async def get_pipeline_status(self) -> Dict[str, Any]:
        """Get overall pipeline status from FLEET-Q"""
        
        response = await self.client.get("/admin/queue")
        response.raise_for_status()
        
        queue_stats = response.json()
        
        # Get leader information
        leader_response = await self.client.get("/admin/leader")
        leader_response.raise_for_status()
        leader_info = leader_response.json()
        
        return {
            "pipeline_id": self.pipeline_id,
            "queue_stats": queue_stats,
            "leader_info": leader_info,
            "timestamp": datetime.utcnow().isoformat()
        }
    
    async def cleanup(self):
        """Cleanup resources"""
        await self.client.aclose()


class PipelineMonitor:
    """Monitor pipeline execution and provide real-time status"""
    
    def __init__(self, pipeline: FinancialDataPipeline):
        self.pipeline = pipeline
        self.monitoring = False
    
    async def start_monitoring(self, update_interval: int = 10):
        """Start real-time pipeline monitoring"""
        
        self.monitoring = True
        
        logger.info("Starting pipeline monitoring", update_interval=update_interval)
        
        while self.monitoring:
            try:
                status = await self.pipeline.get_pipeline_status()
                
                print(f"\r📊 Queue: {status['queue_stats']['pending_steps']} pending, "
                      f"{status['queue_stats']['claimed_steps']} processing, "
                      f"{status['queue_stats']['completed_steps']} completed, "
                      f"{status['queue_stats']['failed_steps']} failed | "
                      f"Leader: {status['leader_info']['current_leader']}", end="")
                
                await asyncio.sleep(update_interval)
                
            except Exception as e:
                logger.error("Monitoring error", error=str(e))
                await asyncio.sleep(update_interval)
    
    def stop_monitoring(self):
        """Stop pipeline monitoring"""
        self.monitoring = False
        print("\n📊 Monitoring stopped")


async def run_financial_pipeline_example():
    """Run the complete financial pipeline example"""
    
    print("🏦 Financial Data Pipeline with FLEET-Q")
    print("=" * 60)
    
    # Initialize pipeline
    pipeline = FinancialDataPipeline()
    monitor = PipelineMonitor(pipeline)
    
    try:
        # Start monitoring in background
        monitor_task = asyncio.create_task(monitor.start_monitoring(5))
        
        # Run pipeline for today's date
        report_date = date.today()
        
        print(f"🚀 Starting daily pipeline for {report_date}")
        
        # Run the complete pipeline
        result = await pipeline.run_daily_pipeline(report_date)
        
        # Stop monitoring
        monitor.stop_monitoring()
        await monitor_task
        
        print(f"\n✅ Pipeline completed successfully!")
        print(f"   Duration: {result['duration_seconds']:.1f} seconds")
        print(f"   Steps completed: {result['steps_completed']}")
        
        # Get final status
        final_status = await pipeline.get_pipeline_status()
        print(f"\n📈 Final Queue Status:")
        print(f"   Pending: {final_status['queue_stats']['pending_steps']}")
        print(f"   Completed: {final_status['queue_stats']['completed_steps']}")
        print(f"   Failed: {final_status['queue_stats']['failed_steps']}")
        
        return result
        
    except Exception as e:
        monitor.stop_monitoring()
        print(f"\n❌ Pipeline failed: {e}")
        raise
    
    finally:
        await pipeline.cleanup()


async def demonstrate_error_scenarios():
    """Demonstrate how FLEET-Q handles various error scenarios"""
    
    print("\n🚨 Error Handling Scenarios")
    print("-" * 40)
    
    pipeline = FinancialDataPipeline()
    
    try:
        # Scenario 1: Submit step with invalid payload
        print("Testing invalid step submission...")
        try:
            await pipeline.submit_step("invalid_step_type", {})
        except Exception as e:
            print(f"✅ Invalid step handled: {type(e).__name__}")
        
        # Scenario 2: Submit step that will fail and retry
        print("Testing step failure and retry...")
        step_id = await pipeline.submit_step(
            "test_failure",
            {"failure_rate": 0.8, "max_attempts": 3},
            max_retries=3
        )
        
        try:
            status = await pipeline.wait_for_step(step_id, timeout_seconds=60)
            print(f"✅ Step with retries: {status['status']} (retries: {status.get('retry_count', 0)})")
        except Exception as e:
            print(f"❌ Step failed after retries: {e}")
        
        # Scenario 3: Test timeout handling
        print("Testing timeout handling...")
        try:
            await pipeline.wait_for_step("nonexistent_step", timeout_seconds=5)
        except Exception as e:
            print(f"✅ Timeout handled: {type(e).__name__}")
        
    finally:
        await pipeline.cleanup()


async def main():
    """Main example function"""
    
    # Check if FLEET-Q is running
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get("http://localhost:8000/health")
            response.raise_for_status()
            print("✅ FLEET-Q is running and healthy")
    except Exception as e:
        print(f"❌ FLEET-Q is not accessible: {e}")
        print("Please start FLEET-Q with: fleet-q")
        return
    
    # Run the financial pipeline example
    try:
        await run_financial_pipeline_example()
        await demonstrate_error_scenarios()
        
        print("\n🎉 Real-world integration example completed!")
        print("\nThis example demonstrated:")
        print("  • Complex multi-step pipeline coordination")
        print("  • Parallel and sequential step execution")
        print("  • Error handling and retry mechanisms")
        print("  • Real-time monitoring and status tracking")
        print("  • Production-ready logging and observability")
        
    except KeyboardInterrupt:
        print("\n👋 Example interrupted by user")
    except Exception as e:
        print(f"\n❌ Example failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())