
public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int[] nums={1,2,3,4};
		for(int i=1;i<Math.pow(2, nums.length);i++){
			String num=Integer.toBinaryString(i);
			System.out.print(num+"--");
			for(int j =num.length()-1;j>=0;j--){
				if(num.charAt(j)=='1'){
					System.out.print(nums[num.length()-1-j]);
				}
			}
			System.out.println();
		}
		
	}

}
