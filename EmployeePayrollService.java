import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class EmployeePayrollService {
	public enum IOService {CONSOLE_IO, FILE_IO, DB_IO, REST_IO}
	private List<EmployeePayrollData> employeePayrollList;
	private EmployeePayrollDBService employeePayrollDBService;

	public EmployeePayrollService() {
		employeePayrollDBService = EmployeePayrollDBService.getInstance();
	}

	public EmployeePayrollService(List<EmployeePayrollData> employeePayrollList) {
		this();
		this.employeePayrollList = employeePayrollList;
	}

	public static void main(String[] args){
		EmployeePayrollService  employeePayrollService =new EmployeePayrollService();
		Scanner consoleInputReader = new Scanner(System.in);
		employeePayrollService.readEmployeePayrollData(consoleInputReader);
		employeePayrollService.writeEmployeePayrollData(IOService.CONSOLE_IO);
	}


	public List<EmployeePayrollData> readEmployeePayrollData(Scanner consoleInputReader) {
		// TODO Auto-generated method stub
		System.out.println("Enter Employee ID: ");
		int id =  consoleInputReader.nextInt();
		System.out.println("Enter Employee Name: ");
		String name = consoleInputReader.next();
		System.out.println("Enter Employee Salary:");
		double salary = consoleInputReader.nextDouble();
		employeePayrollList.add(new EmployeePayrollData(id,name, salary));
		return employeePayrollList; 	

	}

	public List<EmployeePayrollData> readEmployeePayrollData(IOService ioService) {
		if(ioService.equals(IOService.DB_IO))
			this.employeePayrollList =   employeePayrollDBService.readData();
		return this.employeePayrollList;
	}

	public List<EmployeePayrollData> readEmployeePayrollForDateRange(IOService ioService, LocalDate startDate, LocalDate endDate) {
		// TODO Auto-generated method stub
		if(ioService.equals(IOService.DB_IO))
			return employeePayrollDBService.getEmployeePayrollForDateRange(startDate, endDate);
		return null;
	}

	public Map<String, Double> readAverageSalaryByGender(IOService ioService) {
		// TODO Auto-generated method stub
		if(ioService.equals(IOService.DB_IO))
			return employeePayrollDBService.getAverageSalaryByGender();
		return null;
	}



	public void updateEmployeeSalary(String name, double salary) {
		// TODO Auto-generated method stub
		int result = new EmployeePayrollDBService().updateEmployeeData(name,salary);
		if (result == 0) return;
		EmployeePayrollData employeePayrollData = this.getEmployeePayrollData(name);
		if (employeePayrollData != null) employeePayrollData.salary = salary;

	}

	private EmployeePayrollData getEmployeePayrollData(String name) {
		// TODO Auto-generated method stub
		return this.employeePayrollList.stream()
				.filter(employeePayrollDataItem -> employeePayrollDataItem.name.equals(name))
				.findFirst()
				.orElse(null);
	}


	public void addEmployeeToPayroll(List<EmployeePayrollData> employeePayrollDataList) {
		// TODO Auto-generated method stub
		employeePayrollDataList.forEach(employeePayrollData -> {
			System.out.println("Employee Being Added: " +employeePayrollData.name);
			this.addEmployeeToPayroll(employeePayrollData.name, employeePayrollData.salary, employeePayrollData.startDate, employeePayrollData.gender);
			System.out.println("Employee Added: " +employeePayrollData.name);
		});
		System.out.println(this.employeePayrollList); 
	}

	public void addEmployeeToPayroll(String name, double salary, LocalDate startDate, String gender) {
		// TODO Auto-generated method stub
		employeePayrollList.add(employeePayrollDBService.addEmployeeToPayroll(name, salary, startDate, gender));
	}

	public boolean checkEmployeePayrollInSynWithDB(String name) {
		// TODO Auto-generated method stub
		List<EmployeePayrollData> employeePayrollList = employeePayrollDBService.getEmployeePayrollData(name);

		return employeePayrollList.get(0).equals(getEmployeePayrollData(name));
	}

	public void writeEmployeePayrollData(IOService ioService) {
		// TODO Auto-generated method stub
		if(ioService.equals(ioService.CONSOLE_IO))
			System.out.println("\nwriting Employee Payroll Roaster to console" + employeePayrollList);
		else if (ioService.equals(ioService.FILE_IO))
			new EmployeePayrollFileIOService().writeData(employeePayrollList, ioService);

	}

	public void addEmployeeToPayrollWithThreads(List<EmployeePayrollData> employeePayrollDataList) {
		// TODO Auto-generated method stub
		Map<Integer, Boolean> employeeAdditionStatus = new HashMap<Integer, Boolean>();
		employeePayrollDataList.forEach(employeePayrollData -> {
			Runnable task = () -> {
				employeeAdditionStatus.put(employeePayrollData.hashCode(),false);
				System.out.println("Employee Begin Added:" +Thread.currentThread().getName());
				this.addEmployeeToPayroll(employeePayrollData.name, employeePayrollData.salary, 
						employeePayrollData.startDate, employeePayrollData.gender);
				employeeAdditionStatus.put(employeePayrollData.hashCode(), true);
				System.out.println("Employee Added: " +Thread.currentThread().getName());
			};
			Thread thread = new Thread(task, employeePayrollData.name);
			thread.start();
		});
		while(employeeAdditionStatus.containsValue(false)) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		System.out.println(employeePayrollDataList);
	}

	public void printData(IOService ioService) {
		if(ioService.equals(IOService.FILE_IO))
			new EmployeePayrollFileIOService().printData();
		else System.out.println(employeePayrollList);
	}

	public long countEntries(IOService ioService) {
		if(ioService.equals(IOService.FILE_IO))
			new EmployeePayrollFileIOService().countEntries();
		return employeePayrollList.size();
	}

}
