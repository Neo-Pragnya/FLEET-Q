#!/usr/bin/env python3
"""
Integration test runner for FLEET-Q.

This script runs the comprehensive integration test suite including:
- Multi-pod simulation tests
- End-to-end workflow validation
- Failure injection and recovery tests
- Performance and load tests

Usage:
    python scripts/run_integration_tests.py [options]

Options:
    --suite SUITE    Run specific test suite (integration, performance, all)
    --verbose        Enable verbose output
    --parallel       Run tests in parallel where possible
    --report         Generate detailed test report
    --timeout SECS   Set test timeout (default: 300)
"""

import argparse
import asyncio
import sys
import time
import subprocess
from pathlib import Path
from typing import List, Dict, Any
import json


class IntegrationTestRunner:
    """
    Comprehensive integration test runner for FLEET-Q.
    
    Coordinates execution of all integration test suites and provides
    detailed reporting on test results and performance metrics.
    """
    
    def __init__(self, verbose: bool = False, timeout: int = 300):
        self.verbose = verbose
        self.timeout = timeout
        self.results: Dict[str, Any] = {}
        self.start_time = None
        
    def run_test_suite(self, suite_name: str, test_files: List[str]) -> Dict[str, Any]:
        """Run a specific test suite and return results."""
        print(f"\n{'='*60}")
        print(f"Running {suite_name} Test Suite")
        print(f"{'='*60}")
        
        suite_start = time.time()
        suite_results = {
            'name': suite_name,
            'files': test_files,
            'start_time': suite_start,
            'results': {},
            'summary': {}
        }
        
        for test_file in test_files:
            print(f"\nExecuting: {test_file}")
            file_result = self._run_pytest_file(test_file)
            suite_results['results'][test_file] = file_result
            
            if self.verbose:
                self._print_file_results(test_file, file_result)
        
        suite_end = time.time()
        suite_results['end_time'] = suite_end
        suite_results['duration'] = suite_end - suite_start
        
        # Calculate suite summary
        suite_results['summary'] = self._calculate_suite_summary(suite_results['results'])
        
        self._print_suite_summary(suite_name, suite_results['summary'])
        
        return suite_results
    
    def _run_pytest_file(self, test_file: str) -> Dict[str, Any]:
        """Run pytest on a specific file and capture results."""
        cmd = [
            sys.executable, "-m", "pytest",
            test_file,
            "-v",
            "--tb=short",
            "--json-report",
            "--json-report-file=/tmp/pytest_report.json",
            f"--timeout={self.timeout}"
        ]
        
        if not self.verbose:
            cmd.append("-q")
        
        start_time = time.time()
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.timeout + 30  # Add buffer to subprocess timeout
            )
            
            end_time = time.time()
            
            # Try to load JSON report
            json_report = {}
            try:
                with open("/tmp/pytest_report.json", "r") as f:
                    json_report = json.load(f)
            except Exception:
                pass
            
            return {
                'file': test_file,
                'start_time': start_time,
                'end_time': end_time,
                'duration': end_time - start_time,
                'return_code': result.returncode,
                'stdout': result.stdout,
                'stderr': result.stderr,
                'json_report': json_report,
                'success': result.returncode == 0
            }
            
        except subprocess.TimeoutExpired:
            return {
                'file': test_file,
                'start_time': start_time,
                'end_time': time.time(),
                'duration': self.timeout,
                'return_code': -1,
                'stdout': "",
                'stderr': f"Test timed out after {self.timeout} seconds",
                'json_report': {},
                'success': False,
                'timeout': True
            }
        except Exception as e:
            return {
                'file': test_file,
                'start_time': start_time,
                'end_time': time.time(),
                'duration': time.time() - start_time,
                'return_code': -2,
                'stdout': "",
                'stderr': f"Error running test: {e}",
                'json_report': {},
                'success': False,
                'error': str(e)
            }
    
    def _calculate_suite_summary(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate summary statistics for a test suite."""
        total_files = len(results)
        successful_files = sum(1 for r in results.values() if r['success'])
        failed_files = total_files - successful_files
        
        total_duration = sum(r['duration'] for r in results.values())
        
        # Extract test counts from JSON reports
        total_tests = 0
        passed_tests = 0
        failed_tests = 0
        skipped_tests = 0
        
        for result in results.values():
            json_report = result.get('json_report', {})
            summary = json_report.get('summary', {})
            
            total_tests += summary.get('total', 0)
            passed_tests += summary.get('passed', 0)
            failed_tests += summary.get('failed', 0)
            skipped_tests += summary.get('skipped', 0)
        
        return {
            'total_files': total_files,
            'successful_files': successful_files,
            'failed_files': failed_files,
            'total_duration': total_duration,
            'total_tests': total_tests,
            'passed_tests': passed_tests,
            'failed_tests': failed_tests,
            'skipped_tests': skipped_tests,
            'success_rate': passed_tests / total_tests if total_tests > 0 else 0
        }
    
    def _print_file_results(self, test_file: str, result: Dict[str, Any]) -> None:
        """Print detailed results for a test file."""
        status = "✓ PASSED" if result['success'] else "✗ FAILED"
        duration = result['duration']
        
        print(f"  {status} - {test_file} ({duration:.2f}s)")
        
        if not result['success']:
            print(f"    Return code: {result['return_code']}")
            if result.get('timeout'):
                print(f"    TIMEOUT after {self.timeout}s")
            if result['stderr']:
                print(f"    Error: {result['stderr'][:200]}...")
    
    def _print_suite_summary(self, suite_name: str, summary: Dict[str, Any]) -> None:
        """Print summary for a test suite."""
        print(f"\n{suite_name} Suite Summary:")
        print(f"  Files: {summary['successful_files']}/{summary['total_files']} passed")
        print(f"  Tests: {summary['passed_tests']}/{summary['total_tests']} passed")
        print(f"  Success Rate: {summary['success_rate']:.1%}")
        print(f"  Duration: {summary['total_duration']:.2f}s")
        
        if summary['failed_tests'] > 0:
            print(f"  ⚠️  {summary['failed_tests']} tests failed")
        if summary['skipped_tests'] > 0:
            print(f"  ⏭️  {summary['skipped_tests']} tests skipped")
    
    def run_integration_tests(self) -> Dict[str, Any]:
        """Run integration test suite."""
        integration_files = [
            "tests/test_integration_suite.py"
        ]
        
        return self.run_test_suite("Integration", integration_files)
    
    def run_performance_tests(self) -> Dict[str, Any]:
        """Run performance test suite."""
        performance_files = [
            "tests/test_performance_load.py"
        ]
        
        return self.run_test_suite("Performance", performance_files)
    
    def run_all_tests(self) -> Dict[str, Any]:
        """Run all integration and performance tests."""
        print("🚀 Starting FLEET-Q Comprehensive Integration Test Suite")
        print(f"Timeout: {self.timeout}s per test file")
        
        self.start_time = time.time()
        
        # Run integration tests
        integration_results = self.run_integration_tests()
        self.results['integration'] = integration_results
        
        # Run performance tests
        performance_results = self.run_performance_tests()
        self.results['performance'] = performance_results
        
        # Calculate overall summary
        overall_summary = self._calculate_overall_summary()
        self.results['overall'] = overall_summary
        
        self._print_overall_summary(overall_summary)
        
        return self.results
    
    def _calculate_overall_summary(self) -> Dict[str, Any]:
        """Calculate overall test run summary."""
        total_duration = time.time() - self.start_time
        
        total_files = 0
        successful_files = 0
        total_tests = 0
        passed_tests = 0
        failed_tests = 0
        skipped_tests = 0
        
        for suite_name, suite_results in self.results.items():
            if suite_name == 'overall':
                continue
                
            summary = suite_results['summary']
            total_files += summary['total_files']
            successful_files += summary['successful_files']
            total_tests += summary['total_tests']
            passed_tests += summary['passed_tests']
            failed_tests += summary['failed_tests']
            skipped_tests += summary['skipped_tests']
        
        return {
            'total_duration': total_duration,
            'total_suites': len([k for k in self.results.keys() if k != 'overall']),
            'total_files': total_files,
            'successful_files': successful_files,
            'failed_files': total_files - successful_files,
            'total_tests': total_tests,
            'passed_tests': passed_tests,
            'failed_tests': failed_tests,
            'skipped_tests': skipped_tests,
            'overall_success_rate': passed_tests / total_tests if total_tests > 0 else 0,
            'all_passed': failed_tests == 0
        }
    
    def _print_overall_summary(self, summary: Dict[str, Any]) -> None:
        """Print overall test run summary."""
        print(f"\n{'='*60}")
        print("🏁 FLEET-Q Integration Test Suite Complete")
        print(f"{'='*60}")
        
        print(f"Overall Results:")
        print(f"  Suites: {summary['total_suites']}")
        print(f"  Files: {summary['successful_files']}/{summary['total_files']} passed")
        print(f"  Tests: {summary['passed_tests']}/{summary['total_tests']} passed")
        print(f"  Success Rate: {summary['overall_success_rate']:.1%}")
        print(f"  Total Duration: {summary['total_duration']:.2f}s")
        
        if summary['all_passed']:
            print("\n🎉 All tests passed! FLEET-Q integration is working correctly.")
        else:
            print(f"\n❌ {summary['failed_tests']} tests failed. Please review the failures above.")
            
        if summary['skipped_tests'] > 0:
            print(f"ℹ️  {summary['skipped_tests']} tests were skipped.")
    
    def generate_report(self, output_file: str = "integration_test_report.json") -> None:
        """Generate detailed JSON report of test results."""
        report = {
            'timestamp': time.time(),
            'test_run_summary': self.results.get('overall', {}),
            'suite_results': {k: v for k, v in self.results.items() if k != 'overall'},
            'environment': {
                'python_version': sys.version,
                'platform': sys.platform
            }
        }
        
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"\n📊 Detailed report saved to: {output_file}")


def main():
    """Main entry point for integration test runner."""
    parser = argparse.ArgumentParser(
        description="Run FLEET-Q integration test suite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/run_integration_tests.py --suite all --verbose
  python scripts/run_integration_tests.py --suite integration --report
  python scripts/run_integration_tests.py --suite performance --timeout 600
        """
    )
    
    parser.add_argument(
        '--suite',
        choices=['integration', 'performance', 'all'],
        default='all',
        help='Test suite to run (default: all)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    
    parser.add_argument(
        '--report',
        action='store_true',
        help='Generate detailed JSON report'
    )
    
    parser.add_argument(
        '--timeout',
        type=int,
        default=300,
        help='Test timeout in seconds (default: 300)'
    )
    
    args = parser.parse_args()
    
    # Change to project root directory
    project_root = Path(__file__).parent.parent
    import os
    os.chdir(project_root)
    
    # Create test runner
    runner = IntegrationTestRunner(
        verbose=args.verbose,
        timeout=args.timeout
    )
    
    # Run requested test suite
    try:
        if args.suite == 'integration':
            results = runner.run_integration_tests()
        elif args.suite == 'performance':
            results = runner.run_performance_tests()
        else:  # all
            results = runner.run_all_tests()
        
        # Generate report if requested
        if args.report:
            runner.generate_report()
        
        # Exit with appropriate code
        overall = results.get('overall', {})
        if overall.get('all_passed', False):
            sys.exit(0)
        else:
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Test run interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n\n💥 Test runner error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()