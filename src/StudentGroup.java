import java.util.Date;
import java.util.Scanner;
import java.text.SimpleDateFromat;

/**
 * A fix-sized array of students
 * array length should always be equal to the number of stored elements
 * after the element was removed the size of the array should be equal to the number of stored elements
 * after the element was added the size of the array should be equal to the number of stored elements
 * null elements are not allowed to be stored in the array
 *
 * You may add new methods, fields to this class, but DO NOT RENAME any given class, interface or method
 * DO NOT PUT any classes into packages
 *
 */
public class StudentGroup implements StudentArrayOperation {

	private Student[] students;
	Student s;

    Scanner sc=new Scanner(System.in);

	/**
	 * DO NOT remove or change this constructor, it will be used during task check
	 * @param length
	 */
	public StudentGroup(int length) {
		this.students = new Student[length];
	}

	@Override
	public Student[] getStudents() {
		// Add your implementation here

		return students;
	}

	@Override
	public void setStudents(Student[] students) throws IllegalArgumentException {
		// Add your implementation here
        this.students=students;
	}

	@Override
	public Student getStudent(int index) throws IllegalArgumentException{
		// Add your implementation here
		int id = students[index].getId();
		String fullName = students[index].getFullName();
		Date birthDate = students[index].getBirthDate();
		double avgMark = students[index].getAvgMark();
        s=new Student(id, fullName, birthDate, avgMark);
		return s;
	}

	@Override
	public void setStudent(Student student, int index) throws IllegalArgumentException {
		// Add your implementation here
		//System.out.print("Enter id");
		int id = students[index].setId(sc.nextInt());
		//System.out.print("Enter Full Name");
		String fullName = students[index].setFullName(sc.nextLine());
		//System.out.print("Enter birth date");
		String s = new String();
		s=sc.nextLine();
		SimpleDateFormat sdf=new SimpleDateFormat("dd/mm/yyyy");
		Date date=new Date();
		date=sdf.parse(s);
		Date birthDate = students[index].setBirthDate(date);
		//System.out.print("Enter average mark");
		double avgMark = students[index].setAvgMark(sc.nextDouble());
	}

	@Override
	public void addFirst(Student student) throws IllegalArgumentException {
		// Add your implementation here
        for(int i=length;i>0;i--){
            students[i]=students[i-1]
        }
        students[0]=student;
        length++;
	}

	@Override
	public void addLast(Student student) throws IllegalArgumentException {
		// Add your implementation here
        students[length]=student;
        length++;
	}

	@Override
	public void add(Student student, int index) throws IllegalArgumentException {
		// Add your implementation here
        for(int i=length;i>=index;i--){
            students[i]=students[i-1];
        }
        length++;
        students[index]=student;
	}

	@Override
	public void remove(int index) throws IllegalArgumentException {
		// Add your implementation here
        for(int i=index;i<length;i++){
            students[i]=students[i+1];
        }
        length--;
	}

	@Override
	public void remove(Student student) throws IllegalArgumentException {
		// Add your implementation here
        for(int i=0;i<length;i++){
            if(students[i].getId()==student.getId() && students[i].getFullName().equals(student.getFullName()) && students[i].getBirthDate().equals(student.getBirthDate()) && students[i].getAvgMark()==student.getAvgMark()){
                for(int j=i;j<length;j++){
                    students[j]=students[j+1];
                }
                break;
            }
        }
        if(i==length)
            System.out.println("Student not exist");
        length--;
	}

	@Override
	public void removeFromIndex(int index) throws IllegalArgumentException {
		// Add your implementation here
		for(int i=index;i<length;i++){
            students[i]=students[i+1];
		}
		length--;
	}

	@Override
	public void removeFromElement(Student student) throws IllegalArgumentException {
		// Add your implementation here
         for(int i=0;i<length;i++){
            if(students[i].getId()!=student.getId() && students[i].getFullName().notEquals(student.getFullName()) && students[i].getBirthDate().notEquals(student.getBirthDate()) && students[i].getAvgMark()!=student.getAvgMark()){
                for(int j=i;j<length;j++){
                    students[j]=students[j+1];
                }
                length--;
                break;
            }
        }
	}

	@Override
	public void removeToIndex(int index) {
		// Add your implementation here

	}

	@Override
	public void removeToElement(Student student) {
		// Add your implementation here
	}

	@Override
	public void bubbleSort() {
		// Add your implementation here
        int i,j;
        Student temp;
        for(i=0;i<length;i++){
            for(j=(i+1);j<length;j++){
                if(students[i].getId()>students[j].getId()){
                    temp=students[i];
                    students[i]=students[j];
                    students[j]=temp;
                }
            }
        }
	}

	@Override
	public Student[] getByBirthDate(Date date) {
		// Add your implementation here
		return null;
	}

	@Override
	public Student[] getBetweenBirthDates(Date firstDate, Date lastDate) {
		// Add your implementation here
		return null;
	}

	@Override
	public Student[] getNearBirthDate(Date date, int days) {
		// Add your implementation here
		return null;
	}

	@Override
	public int getCurrentAgeByDate(int indexOfStudent) {
		// Add your implementation here
		return 0;
	}

	@Override
	public Student[] getStudentsByAge(int age) {
		// Add your implementation here
		return null;
	}

	@Override
	public Student[] getStudentsWithMaxAvgMark() {
		// Add your implementation here

		return null;
	}

	@Override
	public Student getNextStudent(Student student) {
		// Add your implementation here
		return null;
	}
}
