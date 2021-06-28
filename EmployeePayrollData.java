import java.time.LocalDate;

public class EmployeePayrollData {

	public int id;
	public String name;
	public String gender;
	public double salary;
	public LocalDate startDate;

	public EmployeePayrollData(int employee_id, String name, double salary) {
		// TODO Auto-generated constructor stub
		this.id =  employee_id;
		this.name = name;
		this.salary = salary;
	}

	public EmployeePayrollData(int employee_id, String name, double salary, LocalDate startDate) {
		this(employee_id,name,salary);
		this.startDate = startDate;
	}
	public EmployeePayrollData(int id, String name, String gender, double salary, LocalDate startDate) {
		// TODO Auto-generated constructor stub
	this(id, name, salary, startDate);
	this.gender = gender;
	}
	
	

	@Override
	public String toString() {
		return "EmployeePayrollData [id=" + id + ", name=" + name + ", salary=" + salary + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((gender == null) ? 0 : gender.hashCode());
		result = prime * result + id;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		long temp;
		temp = Double.doubleToLongBits(salary);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + ((startDate == null) ? 0 : startDate.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		EmployeePayrollData other = (EmployeePayrollData) obj;
		if (gender == null) {
			if (other.gender != null)
				return false;
		} else if (!gender.equals(other.gender))
			return false;
		if (id != other.id)
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (Double.doubleToLongBits(salary) != Double.doubleToLongBits(other.salary))
			return false;
		if (startDate == null) {
			if (other.startDate != null)
				return false;
		} else if (!startDate.equals(other.startDate))
			return false;
		return true;
	}
}
