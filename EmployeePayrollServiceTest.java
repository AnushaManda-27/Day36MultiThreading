import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import com.multiThread.EmployeePayrollService.IOService;


class EmployeePayrollServiceTest {
	private IOService fileIOService = IOService.FILE_IO;
	private LocalDate localData;

	@Test
	public void given3EmployeesWhenWrittenToFileShouldMatchEmployeeEntries() {
		EmployeePayrollData[] arrayOfEmps = {
				new EmployeePayrollData(1, "Bill", 10000.0),
				new EmployeePayrollData(2, "Teria", 20000.0),
				new EmployeePayrollData(3, "Charlie", 30000.0),
		};

		EmployeePayrollService employeePayrollService = new EmployeePayrollService(Arrays.asList(arrayOfEmps));
		employeePayrollService.writeEmployeePayrollData(fileIOService);
		employeePayrollService.printData(fileIOService);
		long entries = employeePayrollService.countEntries(fileIOService);
		//	Assert.assertEquals(3, entries);
	}

	@Test
	public void givenEmployeePayrollInDB_WhenRetrieved_ShouldMatchEmployeeCount() {
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		List<EmployeePayrollData> employeePayrollData = employeePayrollService.readEmployeePayrollData(IOService.DB_IO);
		Assertrtion.assertEquals(3, employeePayrollData.size());

	}

	@Test
	public void givenNewSalaryForEmployee_WhenUpdatedShouldSyncWithDB() {
		EmployeePayrollService employeePayrollService = new EmployeePayrollService ();
		List<EmployeePayrollData> employeePayrollData = employeePayrollService.readEmployeePayrollData(fileIOService);
		employeePayrollService.updateEmployeeSalary("Teria", 30000.0);
		boolean result = employeePayrollService.checkEmployeePayrollInSynWithDB("Teria");
		Assert.assertTrue(result);
	}

	@Test
	public void givenDateRange_WhenRetrieved_ShouldMatchEmployeeCount() {
		EmployeePayrollService employeePayrollService = new EmployeePayrollService ();
		employeePayrollService.readEmployeePayrollData(IOService.DB_IO);
		LocalDate startDate = LocalDate.of(2018, 01, 01);
		LocalDate endDate = LocalDate.now();
		List<EmployeePayrollData> employeePayrollData = 
				employeePayrollService.readEmployeePayrollForDateRange(IOService.DB_IO, startDate, endDate);
		Assert.assertEquals(3, employeePayrollData.size());
	}

	@Test
	public void givenDateRange_WhenAverageSalaryRetrieveByGender_ShouldReturnProperValue() {
		EmployeePayrollService employeePayrollService = new EmployeePayrollService ();
		employeePayrollService.readEmployeePayrollData(IOService.DB_IO);
		Map<String,Double> averageSalaryByGender = employeePayrollService.readAverageSalaryByGender(IOService.DB_IO);
		Assert.assertTrue(averageSalaryByGender.get("M").equals(20000.0) &&
				averageSalaryByGender.get("F").equals(30000.0));
	}

	@Test
	public void givenNewEmployee_whenAdded_ShouldSyncWithDB() {
		EmployeePayrollService employeePayrollService = new EmployeePayrollService ();
		employeePayrollService.readEmployeePayrollData(IOService.DB_IO);
		employeePayrollService.addEmployeeToPayroll("Mark",50000.00, LocalDate.now(),"M");
		boolean result = employeePayrollService.checkEmployeePayrollInSynWithDB("Mark");
		Assert.assertTrue(result);
	}
	
	@Test
	public void given6Employees_WhenAddedToDB_ShouldMatchEmployeeEntries() {
	EmployeePayrollData[] arrayOfEmps = {
	new EmployeePayrollData(0, "Jeff BenZos", "M", 10000.0, localData.now()),
	new EmployeePayrollData(0, "Bill Gates","M", 20000.0, localData.now()),
	new EmployeePayrollData(0, "Mark Zuckerberg", "M", 30000.0, localData.now()),
	new EmployeePayrollData(0, "Sunder pichai","M", 60000.0, localData.now()),
	new EmployeePayrollData(0, "Mukesh","M", 100000.0, localData.now()),
	new EmployeePayrollData(0, "Pappu","M", 1200000.0, localData.now()),
	};
	EmployeePayrollService employeePayrollService = new EmployeePayrollService();
	employeePayrollService.readEmployeePayrollData(IOService.DB_IO);
	Instant start = Instant.now();
	employeePayrollService.addEmployeeToPayroll(Arrays.asList(arrayOfEmps));
	Instant end =Instant.now();
	System.out.println("Duration without Thread:" + Duration.between(start, end));
	Instant threadStart = Instant.now();
	employeePayrollService.addEmployeeToPayrollWithThreads(Arrays.asList(arrayOfEmps));
	Instant threadEnd = Instant.now();
	System.out.println("Duration with Thread: " +Duration.between(start, end));
	Assert.assertEquals(6, employeePayrollService.countEntries(IOService.DB_IO));
	
	}

}
